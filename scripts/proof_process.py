"""Bounded child groups for disposable proof tooling (Linux only)."""
import os
import signal
import subprocess


def execute(command, *, timeout, **kwargs):
    """Join the child and terminate its process group on timeout or interruption."""
    process = subprocess.Popen(command, start_new_session=True, **kwargs)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException as error:
        # A build/test command can have children after its immediate parent exits.
        # Kill the complete owned session, then always reap our direct child.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        finally:
            stdout, stderr = process.communicate()
            error.stdout, error.stderr = stdout, stderr
        raise
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
