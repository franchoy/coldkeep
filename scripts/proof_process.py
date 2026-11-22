"""Bounded Linux proof children, including descendants that create sessions."""
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

CLEANUP_SECONDS = 3


class ProcessCleanupError(RuntimeError):
    """The child result cannot qualify while process ownership is uncertain."""


def _identity(pid):
    try:
        fields = Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': pid, 'parent': int(fields[1]), 'start_time': int(fields[19]), 'state': fields[0]}
    except (FileNotFoundError, ProcessLookupError):
        return None


def _descendants(parent, deadline):
    processes = {}
    for path in Path('/proc').iterdir():
        if time.monotonic() >= deadline:
            raise TimeoutError('process inventory deadline')
        if path.name.isdecimal():
            identity = _identity(int(path.name))
            if identity is not None:
                processes[identity['pid']] = identity
    owned = {parent}
    while True:
        found = {pid for pid, identity in processes.items() if identity['parent'] in owned}
        expanded = owned | found
        if expanded == owned:
            return [identity for pid, identity in processes.items() if pid in owned and pid != parent]
        owned = expanded


def _signal_identity(identity, signum):
    """Pin the process before comparing its birth identity; never signal a reused PID."""
    try:
        descriptor = os.pidfd_open(identity['pid'])
    except ProcessLookupError:
        return
    try:
        current = _identity(identity['pid'])
        if current is not None and current['start_time'] == identity['start_time']:
            signal.pidfd_send_signal(descriptor, signum)
    except ProcessLookupError:
        pass
    finally:
        os.close(descriptor)


def _cleanup_descendants(parent):
    deadline = time.monotonic() + CLEANUP_SECONDS
    failures, terminated = [], set()
    remaining = []
    while time.monotonic() < deadline:
        try:
            remaining = _descendants(parent, deadline)
            if not remaining:
                return {'complete': not failures, 'failures': failures,
                        'remaining': [], 'terminated_count': len(terminated)}
            # A second observation finds children forked before SIGSTOP took
            # effect. The subreaper keeps their ancestry even if parents exit.
            for identity in remaining:
                _signal_identity(identity, signal.SIGSTOP)
            remaining = _descendants(parent, deadline)
            for identity in remaining:
                if identity['state'] != 'Z':
                    terminated.add((identity['pid'], identity['start_time']))
                _signal_identity(identity, signal.SIGKILL)
            while True:
                try:
                    pid, _ = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                except ChildProcessError:
                    break
        except (OSError, ValueError, TimeoutError) as error:
            failures.append(type(error).__name__)
            break
        time.sleep(.01)
    return {'complete': False, 'failures': failures or ['descendant cleanup deadline'],
            'remaining': [{'pid': item['pid'], 'start_time': item['start_time']} for item in remaining[:16]],
            'terminated_count': len(terminated)}


def _supervise(report_fd, command):
    """One isolated ownership domain; the importing orchestrator is not a subreaper."""
    report = {'returncode': None, 'start_error': None}
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        # Linux PR_SET_CHILD_SUBREAPER, confined to this disposable supervisor.
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno(), 'cannot establish proof process ownership')
        try:
            child = subprocess.Popen(command, close_fds=True)
        except OSError as error:
            report['start_error'] = type(error).__name__
        else:
            report['returncode'] = child.wait()
    except KeyboardInterrupt:
        report['interrupted'] = True
    except BaseException as error:
        report['supervisor_error'] = type(error).__name__
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        report['process_cleanup'] = _cleanup_descendants(os.getpid())
        try:
            os.write(report_fd, json.dumps(report).encode())
        finally:
            os.close(report_fd)


def _read_report(descriptor):
    os.set_blocking(descriptor, False)
    try:
        return json.loads(os.read(descriptor, 16384))
    except (BlockingIOError, ValueError):
        return {'process_cleanup': {'complete': False, 'failures': ['supervisor report unavailable']}}


def _bounded_abort(process):
    """Ask the owning supervisor to clean; retain uncertainty if it cannot finish."""
    process.send_signal(signal.SIGTERM)
    try:
        stdout, stderr = process.communicate(timeout=CLEANUP_SECONDS + 1)
        return stdout, stderr, False
    except BaseException as error:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        # An escaped child may still hold a pipe if its supervisor failed.
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        return getattr(error, 'stdout', None), getattr(error, 'stderr', None), True


def execute(command, *, timeout, **kwargs):
    """Return CompletedProcess or raise with a nonsecret process_cleanup record.

    A separate Linux subreaper owns this command and its detached descendants.
    Credentials use the scoped child environment; no environment or command
    value enters the supervisor's private in-memory status pipe.
    """
    read_fd, write_fd = os.pipe()
    try:
        inherited = tuple(kwargs.pop('pass_fds', ()))
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                                    '--supervise', str(write_fd), *command],
                                   start_new_session=True, pass_fds=(*inherited, write_fd), **kwargs)
    except BaseException:
        os.close(read_fd)
        raise
    finally:
        os.close(write_fd)
    try:
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except BaseException as error:
            stdout, stderr, uncertain = _bounded_abort(process)
            report = _read_report(read_fd)
            error.stdout, error.stderr = stdout, stderr
            error.process_cleanup = report['process_cleanup']
            if uncertain:
                error.process_cleanup = {'complete': False, 'failures': ['supervisor cleanup deadline'],
                                         'supervisor_report': report.get('process_cleanup')}
            raise
        report = _read_report(read_fd)
        cleanup = report['process_cleanup']
        if report.get('start_error'):
            error = OSError('proof child could not start: ' + report['start_error'])
        elif not cleanup['complete'] or report.get('supervisor_error') or report.get('returncode') is None:
            error = ProcessCleanupError('proof child or descendant cleanup incomplete')
        else:
            result = subprocess.CompletedProcess(command, report['returncode'], stdout, stderr)
            result.process_cleanup = cleanup
            return result
        error.process_cleanup, error.stdout, error.stderr = cleanup, stdout, stderr
        raise error
    finally:
        os.close(read_fd)


if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[1] != '--supervise':
        raise SystemExit('internal proof process supervisor only')
    _supervise(int(sys.argv[2]), sys.argv[3:])
