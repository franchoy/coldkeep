package blocks

import (
	"context"
	"database/sql"
	"errors"
)

var ErrBlockAlreadyExists = errors.New("block already exists for chunk")

type Repository struct {
	DB *sql.DB
}

func (r *Repository) Insert(ctx context.Context, tx *sql.Tx, d *Descriptor) error {
	err := tx.QueryRowContext(ctx, `
		INSERT INTO blocks (
			chunk_id,
			codec,
			format_version,
			plaintext_size,
			stored_size,
			nonce,
			container_id,
			block_offset
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (chunk_id) DO NOTHING
		RETURNING id, created_at, updated_at
	`,
		d.ChunkID,
		d.Codec,
		d.FormatVersion,
		d.PlaintextSize,
		d.StoredSize,
		d.Nonce,
		d.ContainerID,
		d.BlockOffset,
	).Scan(
		&d.ID,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return ErrBlockAlreadyExists
	}
	return err
}

func (r *Repository) GetByChunkID(ctx context.Context, chunkID int64) (*Descriptor, error) {
	row := r.DB.QueryRowContext(ctx, `
		SELECT
			id,
			chunk_id,
			codec,
			format_version,
			plaintext_size,
			stored_size,
			nonce,
			container_id,
			block_offset,
			created_at,
			updated_at
		FROM blocks
		WHERE chunk_id = $1
	`, chunkID)

	var d Descriptor

	err := row.Scan(
		&d.ID,
		&d.ChunkID,
		&d.Codec,
		&d.FormatVersion,
		&d.PlaintextSize,
		&d.StoredSize,
		&d.Nonce,
		&d.ContainerID,
		&d.BlockOffset,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (r *Repository) GetByChunkHash(ctx context.Context, chunkHash string) (*Descriptor, error) {
	row := r.DB.QueryRowContext(ctx, `
		SELECT
			b.id,
			b.chunk_id,
			b.codec,
			b.format_version,
			b.plaintext_size,
			b.stored_size,
			b.nonce,
			b.container_id,
			b.block_offset,
			b.created_at,
			b.updated_at
		FROM blocks b
		JOIN chunk c ON c.id = b.chunk_id
		WHERE c.chunk_hash = $1
	`, chunkHash)

	var d Descriptor

	err := row.Scan(
		&d.ID,
		&d.ChunkID,
		&d.Codec,
		&d.FormatVersion,
		&d.PlaintextSize,
		&d.StoredSize,
		&d.Nonce,
		&d.ContainerID,
		&d.BlockOffset,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &d, nil
}
