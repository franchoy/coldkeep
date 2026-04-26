package graph

import (
	"context"
	"database/sql"
)

var _ = context.Background

type Service struct {
	db *sql.DB
}

func NewService(db *sql.DB) *Service {
	return &Service{db: db}
}
