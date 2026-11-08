package recoveryexport

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Tools struct{ PGDump, PGRestore string }
type exportHooks struct {
	childPID func(int)
	cut      *hooks
	event    func(string) error
	write    func(*os.File, []byte) (int, error)
	sync     func(*os.File) error
	close    func(*os.File) error
}

func (h *exportHooks) at(stage string) error {
	if h != nil && h.event != nil {
		return h.event(stage)
	}
	return nil
}
func (h *exportHooks) syncFile(f *os.File) error {
	if h != nil && h.sync != nil {
		return h.sync(f)
	}
	return f.Sync()
}
func (h *exportHooks) closeFile(f *os.File) error {
	if h != nil && h.close != nil {
		return h.close(f)
	}
	return f.Close()
}
func (h *exportHooks) writeFile(f *os.File, b []byte) (int, error) {
	if h != nil && h.write != nil {
		return h.write(f, b)
	}
	return f.Write(b)
}

type childSink struct {
	w        io.Writer
	left     int64
	cancel   context.CancelFunc
	exceeded bool
	failure  error
}

func (s *childSink) Write(p []byte) (int, error) {
	if int64(len(p)) > s.left {
		s.exceeded = true
		s.cancel()
		return 0, ErrLimit
	}
	n, e := s.w.Write(p)
	s.left -= int64(n)
	if e != nil || n != len(p) {
		s.cancel()
		if e == nil {
			e = io.ErrShortWrite
		}
		s.failure = e
	}
	return n, e
}

// Run joins the actual child and os/exec's pipe-copy goroutines. Killing the
// process group interrupts descendants too; neither cancellation nor WaitDelay
// is used as proof that the source lease can be released.
func runChild(ctx context.Context, path string, args, env []string, stdin *os.File, stdout io.Writer, max int64, h *exportHooks) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 5 * time.Second
	cmd.Cancel = func() error {
		e := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(e, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return e
	}
	out := &childSink{w: stdout, left: max, cancel: cancel}
	var stderr bytes.Buffer
	errout := &childSink{w: &stderr, left: 64 << 10, cancel: cancel}
	cmd.Stdout = out
	cmd.Stderr = errout
	if stdin != nil {
		cmd.ExtraFiles = []*os.File{stdin}
	}
	if e := cmd.Start(); e != nil {
		return errors.New("PostgreSQL tool could not start")
	}
	if h != nil && h.childPID != nil {
		h.childPID(cmd.Process.Pid)
	}
	hookErr := h.at("child-started")
	if hookErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	endErr := h.at("child-joined")
	if out.exceeded || errout.exceeded {
		return errors.Join(ErrLimit, hookErr, endErr)
	}
	if waitErr != nil {
		return errors.Join(errors.New("PostgreSQL tool failed; no completion"), ctx.Err(), out.failure, errout.failure, hookErr, endErr)
	}
	return errors.Join(ctx.Err(), out.failure, errout.failure, hookErr, endErr)
}

var toolVersion = regexp.MustCompile(`^pg_(dump|restore) \(PostgreSQL\) (16\.[0-9]+)(?:[ \n]|$)`)

func validateTools(ctx context.Context, t Tools) (dump, restore string, err error) {
	for i, path := range []string{t.PGDump, t.PGRestore} {
		info, e := os.Lstat(path)
		if !filepath.IsAbs(path) || e != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
			return "", "", ErrConfiguration
		}
		var out bytes.Buffer
		if e = runChild(ctx, path, []string{"--version"}, []string{"LC_ALL=C"}, nil, &out, 4096, nil); e != nil {
			return "", "", e
		}
		matches := toolVersion.FindStringSubmatch(out.String())
		want := "dump"
		if i == 1 {
			want = "restore"
		}
		if len(matches) != 3 || matches[1] != want {
			return "", "", ErrUnsupported
		}
		if i == 0 {
			dump = matches[2]
		} else {
			restore = matches[2]
		}
	}
	if dump != restore {
		return "", "", ErrUnsupported
	}
	return
}

var scalarName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var snapshotName = regexp.MustCompile(`^[0-9A-Fa-f]+-[0-9A-Fa-f]+-[0-9]+$`)

// No inherited environment or connection-string-valued dbname is admitted.
// Credentials live only in a private operation-owned passfile outside both trees.
func childCredentials(r resolved, destination string) (env []string, remove func() error, err error) {
	p := r.pg
	if !scalarName.MatchString(p.Database) || !scalarName.MatchString(p.User) || strings.ContainsAny(p.Password, "\r\n\x00") {
		return nil, nil, ErrConfiguration
	}
	dir, e := os.MkdirTemp("/tmp", "coldkeep-export-credentials-")
	if e != nil {
		return nil, nil, e
	}
	remove = func() error { return os.RemoveAll(dir) }
	if overlap(dir, r.identity.CanonicalPath) || overlap(dir, destination) {
		return nil, remove, ErrConfiguration
	}
	pass := filepath.Join(dir, "passfile")
	escape := func(s string) string { return strings.NewReplacer("\\", "\\\\", ":", "\\:").Replace(s) }
	host := p.Host
	if strings.HasPrefix(host, "/") {
		host = "localhost"
	}
	line := escape(host) + ":" + strconv.Itoa(int(p.Port)) + ":" + escape(p.Database) + ":" + escape(p.User) + ":" + escape(p.Password) + "\n"
	f, e := os.OpenFile(pass, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return nil, remove, e
	}
	n, e := io.WriteString(f, line)
	if e == nil && n != len(line) {
		e = io.ErrShortWrite
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return nil, remove, e
	}
	env = []string{"LC_ALL=C", "LANG=C", "HOME=" + dir, "PGPASSFILE=" + pass, "PGCONNECT_TIMEOUT=5", "PGSSLMODE=disable", "PGAPPNAME=coldkeep-recovery-dump", "PGOPTIONS=-c default_transaction_read_only=on -c row_security=off"}
	return env, remove, nil
}
func databaseRequirements(ctx context.Context, tx *sql.Tx) (r DatabaseRequirements, err error) {
	// Full-database dumps are supported only where ownership/ACL remapping suffices.
	// Policies and external objects are refused, never discarded from the dump.
	var unsupported bool
	err = tx.QueryRowContext(ctx, `SELECT
 EXISTS(SELECT FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname !~ '^pg_') OR
 EXISTS(SELECT FROM pg_extension WHERE extname<>'plpgsql') OR
 EXISTS(SELECT FROM pg_policy) OR
 EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND (c.relrowsecurity OR c.relforcerowsecurity OR c.reltablespace<>0)) OR
 EXISTS(SELECT FROM pg_database WHERE datname=current_database() AND (dattablespace<>(SELECT oid FROM pg_tablespace WHERE spcname='pg_default') OR datlocprovider<>'c')) OR
 EXISTS(SELECT FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='public' AND (l.lanname NOT IN ('sql','plpgsql') OR p.prosecdef)) OR
 EXISTS(SELECT FROM pg_foreign_server) OR EXISTS(SELECT FROM pg_largeobject_metadata) OR EXISTS(SELECT FROM pg_event_trigger) OR EXISTS(SELECT FROM pg_publication) OR EXISTS(SELECT FROM pg_seclabel)`).Scan(&unsupported)
	if err != nil {
		return r, errors.New("catalog export prerequisites unavailable")
	}
	if unsupported {
		return r, ErrUnsupported
	}
	err = tx.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int,pg_encoding_to_char(encoding),datcollate,datctype,datlocprovider::text FROM pg_database WHERE datname=current_database()`).Scan(&r.ServerVersion, &r.Encoding, &r.Collate, &r.CType, &r.LocaleProvider)
	r.RestorePolicy = "trusted operator; fresh database; --no-owner --no-acl --exit-on-error"
	return
}
