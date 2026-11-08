package recoveryexport

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"
)

// This connection exists exclusively to hold the database-scoped cooperative
// reservation. It is not an active data session. Cancellation of work must not
// release ownership while child/transaction/credential cleanup is still running.
type importReservation struct {
	db      *sql.DB
	session *catalogSession
	dialer  *catalogDialer
	closed  bool
}

func reserveImport(ctx context.Context, r resolved) (*importReservation, error) {
	cfg := r.pg
	cfg.ApplicationName = "coldkeep-import-reservation"
	connector, e := pq.NewConnectorConfig(cfg)
	if e != nil {
		return nil, ErrConfiguration
	}
	dialer := &catalogDialer{ctx: ctx}
	connector.Dialer(dialer)
	session, e := openCatalogSession(ctx, connector)
	if e != nil {
		return nil, errors.Join(e, dialer.join())
	}
	// Startup is cancellable. Once connected, detach its cancellation callback:
	// idle reservation ownership must survive canceled work until cleanup joins.
	for _, socket := range dialer.sockets {
		if !socket.stop() {
			<-socket.canceled
		}
		socket.stop = func() bool { return true }
	}
	db := sql.OpenDB(session)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	rsv := &importReservation{db: db, session: session, dialer: dialer}
	var acquired bool
	if e = rsv.guard(ctx, func() error {
		return db.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", importLock).Scan(&acquired)
	}); e != nil || !acquired {
		return nil, errors.Join(errors.New("destination database reserved or unavailable"), rsv.close())
	}
	return rsv, nil
}
func (r *importReservation) check(ctx context.Context) error {
	var held bool
	e := r.guard(ctx, func() error {
		return r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid=$1::oid AND objid=$2::oid AND objsubid=1)`, importLock>>32, importLock&0xffffffff).Scan(&held)
	})
	if e != nil || !held {
		return errors.New("destination reservation lost")
	}
	return nil
}
func (r *importReservation) close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return errors.Join(r.session.close(r.db), r.dialer.join())
}

func freshImportTarget(ctx context.Context, db *sql.DB, d Destination, m Manifest) (int64, error) {
	var oid int64
	var name, role, encoding, collate, ctype, provider, tablespace string
	var server int
	var template, owner, unsafeRole, populated bool
	e := db.QueryRowContext(ctx, `SELECT d.oid::bigint,current_database(),current_user,current_setting('server_version_num')::int,
 pg_encoding_to_char(d.encoding),d.datcollate,d.datctype,d.datlocprovider::text,t.spcname,d.datistemplate,
 d.datdba=(SELECT oid FROM pg_roles WHERE rolname=current_user),
 EXISTS(SELECT FROM pg_roles WHERE rolname=current_user AND (rolsuper OR rolreplication OR rolbypassrls OR rolcreatedb OR rolcreaterole OR NOT rolcanlogin OR rolconfig IS NOT NULL)) OR
 EXISTS(SELECT FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=current_user)),
 EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public') OR
 EXISTS(SELECT FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public') OR
 EXISTS(SELECT FROM pg_type p JOIN pg_namespace n ON n.oid=p.typnamespace WHERE n.nspname='public') OR
 EXISTS(SELECT FROM pg_collation WHERE collnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_conversion WHERE connamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_operator WHERE oprnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_opclass WHERE opcnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_opfamily WHERE opfnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_ts_config WHERE cfgnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_ts_dict WHERE dictnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_ts_parser WHERE prsnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_ts_template WHERE tmplnamespace='public'::regnamespace) OR
 EXISTS(SELECT FROM pg_default_acl) OR
 EXISTS(SELECT FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname !~ '^pg_') OR
 EXISTS(SELECT FROM pg_extension WHERE extname<>'plpgsql') OR
 EXISTS(SELECT FROM pg_db_role_setting WHERE setdatabase=d.oid OR setrole=(SELECT oid FROM pg_roles WHERE rolname=current_user)) OR
 EXISTS(SELECT FROM pg_foreign_server) OR EXISTS(SELECT FROM pg_largeobject_metadata) OR EXISTS(SELECT FROM pg_event_trigger) OR
 EXISTS(SELECT FROM pg_publication) OR EXISTS(SELECT FROM pg_seclabel) OR EXISTS(SELECT FROM pg_subscription WHERE subdbid=d.oid)
 FROM pg_database d JOIN pg_tablespace t ON t.oid=d.dattablespace WHERE d.datname=current_database()`).Scan(&oid, &name, &role, &server, &encoding, &collate, &ctype, &provider, &tablespace, &template, &owner, &unsafeRole, &populated)
	if e != nil {
		return 0, errors.New("destination prerequisites unavailable")
	}
	if name != d.Database || role != d.User || name == "postgres" || name == "template0" || name == "template1" || template || !owner || unsafeRole || populated ||
		server < 160000 || server >= 170000 || encoding != m.Database.Encoding || collate != m.Database.Collate || ctype != m.Database.CType || provider != m.Database.LocaleProvider || tablespace != "pg_default" {
		return 0, ErrUnsupported
	}
	return oid, nil
}

// Reservation queries are bounded, while idle ownership has no work-context
// cancellation deadline. Every callback joins before returning to the owner.
func (r *importReservation) guard(ctx context.Context, query func() error) (err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	socket := r.dialer.sockets[0]
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	if err = socket.SetDeadline(deadline); err != nil {
		return
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = socket.SetDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		err = errors.Join(err, socket.SetDeadline(time.Time{}), ctx.Err())
	}()
	return query()
}
func (r *importReservation) fresh(ctx context.Context, d Destination, m Manifest) (oid int64, err error) {
	err = r.guard(ctx, func() error { var e error; oid, e = freshImportTarget(ctx, r.db, d, m); return e })
	return
}
