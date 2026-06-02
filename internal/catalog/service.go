package catalog

import "database/sql"

// Service is the default catalog implementation. It wraps a DB dependency and
// satisfies the Catalog interface.
//
// NewService is the only supported constructor. The caller is responsible for
// the DB connection lifetime.
type Service struct {
	db DB
}

// compile-time interface assertions — all sub-interfaces must remain satisfied.
var (
	_ Catalog              = (*Service)(nil)
	_ LogicalFileCatalog   = (*Service)(nil)
	_ PhysicalFileCatalog  = (*Service)(nil)
	_ SnapshotCatalog      = (*Service)(nil)
	_ SnapshotGraphCatalog = (*Service)(nil)
	_ ReachabilityCatalog  = (*Service)(nil)
	_ PlacementCatalog     = (*Service)(nil)
	_ RestorePlanCatalog   = (*Service)(nil)
	_ GCPlanCatalog        = (*Service)(nil)
)

// NewService constructs a Service backed by the given DB. Both *sql.DB and
// *sql.Tx satisfy the DB interface.
func NewService(db DB) *Service {
	return &Service{db: db}
}

// NewServiceFromSQL is a convenience constructor that accepts a *sql.DB
// directly. This is the common production path.
func NewServiceFromSQL(db *sql.DB) *Service {
	return NewService(db)
}
