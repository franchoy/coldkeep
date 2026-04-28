package observability

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/franchoy/coldkeep/internal/graph"
)

type Service struct {
	db    *sql.DB
	graph *graph.Service
	now   func() time.Time
}

func NewService(db *sql.DB) (*Service, error) {
	if db == nil {
		return nil, fmt.Errorf("observability service requires non-nil db")
	}

	return &Service{
		db:    db,
		graph: graph.NewService(db),
		now:   func() time.Time { return time.Now().UTC() },
	}, nil
}

func newServiceForTest(db *sql.DB, now func() time.Time) *Service {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{db: db, graph: graph.NewService(db), now: now}
}
