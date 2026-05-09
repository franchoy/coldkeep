package capabilities

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/franchoy/coldkeep/internal/db"
)

const (
	repositoryConfigCompressionKey      = "compression"
	repositoryConfigCompressionLevelKey = "compression_level"
)

// Derive loads repository capability semantics from schema and stored metadata.
func Derive(ctx context.Context, dbconn *sql.DB) (RepositoryCapabilities, error) {
	if dbconn == nil {
		return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: nil db connection")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	caps := RepositoryCapabilities{
		DefaultCompression:      CompressionNone,
		DefaultCompressionLevel: 3,
		DefaultEncryption:       EncryptionNone,
		DefaultPacking:          PackingPackedMulti,
		ReadPathMetadataDriven:  true,
	}

	supportedCompression := make(map[string]struct{})
	supportedEncryption := make(map[string]struct{})
	supportedPacking := make(map[string]struct{})
	observedCompression := make(map[string]struct{})
	observedEncryption := make(map[string]struct{})
	observedPacking := make(map[string]struct{})

	caps.RepositoryFormatVersion = readSchemaVersion(ctx, dbconn)

	defaultCompression, defaultCompressionLevel, err := readRepositoryDefaults(ctx, dbconn)
	if err != nil {
		return RepositoryCapabilities{}, err
	}
	caps.DefaultCompression = defaultCompression
	caps.DefaultCompressionLevel = defaultCompressionLevel
	addSet(supportedCompression, defaultCompression)

	hasBlocksTable, err := tableExists(ctx, dbconn, "blocks")
	if err != nil {
		return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: inspect blocks table: %w", err)
	}
	if hasBlocksTable {
		// Legacy block layout is readable when blocks exists.
		addSet(supportedPacking, PackingLegacySingle)
		addSet(supportedCompression, CompressionNone)
		addSet(supportedEncryption, EncryptionNone)
		addSet(supportedEncryption, EncryptionAESGCM)

		if err := collectObservedFromBlocks(ctx, dbconn, observedCompression, observedEncryption, observedPacking); err != nil {
			return RepositoryCapabilities{}, err
		}
	}

	hasStorageBlocksTable, err := tableExists(ctx, dbconn, "storage_blocks")
	if err != nil {
		return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: inspect storage_blocks table: %w", err)
	}
	hasChunkBlockRefsTable := false
	if hasStorageBlocksTable {
		hasChunkBlockRefsTable, err = tableExists(ctx, dbconn, "chunk_block_refs")
		if err != nil {
			return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: inspect chunk_block_refs table: %w", err)
		}
		if hasChunkBlockRefsTable {
			addSet(supportedPacking, PackingPackedMulti)
		}
		addSet(supportedCompression, CompressionNone)
		addSet(supportedCompression, CompressionZstd)
		addSet(supportedEncryption, EncryptionNone)
		addSet(supportedEncryption, EncryptionAESGCM)

		supportsCompressedHash, err := columnExists(ctx, dbconn, "storage_blocks", "compressed_hash")
		if err != nil {
			return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: inspect storage_blocks.compressed_hash: %w", err)
		}
		caps.SupportsCompressedHash = supportsCompressedHash

		supportsPhysicalHash, err := columnExists(ctx, dbconn, "storage_blocks", "physical_hash")
		if err != nil {
			return RepositoryCapabilities{}, fmt.Errorf("derive repository capabilities: inspect storage_blocks.physical_hash: %w", err)
		}
		caps.SupportsPhysicalHash = supportsPhysicalHash

		if err := collectObservedFromStorageBlocks(ctx, dbconn, observedCompression, observedEncryption, observedPacking); err != nil {
			return RepositoryCapabilities{}, err
		}
	}

	if !hasStorageBlocksTable || !hasChunkBlockRefsTable {
		caps.DefaultPacking = PackingLegacySingle
	}

	caps.SupportedCompression = sortedSet(supportedCompression)
	caps.SupportedEncryption = sortedSet(supportedEncryption)
	caps.SupportedPacking = sortedSet(supportedPacking)
	caps.ObservedCompression = sortedSet(observedCompression)
	caps.ObservedEncryption = sortedSet(observedEncryption)
	caps.ObservedPacking = sortedSet(observedPacking)

	return caps, nil
}

func readSchemaVersion(ctx context.Context, dbconn *sql.DB) int {
	var v int
	if err := dbconn.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_version`).Scan(&v); err != nil {
		return 0
	}
	return v
}

func readRepositoryDefaults(ctx context.Context, dbconn *sql.DB) (string, int, error) {
	compression := CompressionNone
	level := 3

	hasRepositoryConfig, err := tableExists(ctx, dbconn, "repository_config")
	if err != nil {
		return "", 0, fmt.Errorf("derive repository capabilities: inspect repository_config table: %w", err)
	}
	if !hasRepositoryConfig {
		return compression, level, nil
	}

	compressionRaw, err := readRepositoryConfigValue(ctx, dbconn, repositoryConfigCompressionKey)
	if err != nil && err != sql.ErrNoRows {
		return "", 0, fmt.Errorf("derive repository capabilities: read repository default compression: %w", err)
	}
	if compressionRaw.Valid {
		trimmed := strings.TrimSpace(compressionRaw.String)
		if trimmed != "" {
			compression = trimmed
		}
	}

	levelRaw, err := readRepositoryConfigValue(ctx, dbconn, repositoryConfigCompressionLevelKey)
	if err != nil && err != sql.ErrNoRows {
		return "", 0, fmt.Errorf("derive repository capabilities: read repository default compression level: %w", err)
	}
	if levelRaw.Valid {
		trimmed := strings.TrimSpace(levelRaw.String)
		if trimmed != "" {
			parsed, parseErr := strconv.Atoi(trimmed)
			if parseErr != nil {
				return "", 0, fmt.Errorf("derive repository capabilities: parse compression level %q: %w", trimmed, parseErr)
			}
			level = parsed
		}
	}

	return compression, level, nil
}

func readRepositoryConfigValue(ctx context.Context, dbconn *sql.DB, key string) (sql.NullString, error) {
	var value sql.NullString
	var err error

	switch db.BackendFromDB(dbconn) {
	case db.BackendPostgres:
		err = dbconn.QueryRowContext(ctx, `
			SELECT value
			FROM repository_config
			WHERE key = $1
		`, key).Scan(&value)
	case db.BackendSQLite:
		err = dbconn.QueryRowContext(ctx, `
			SELECT value
			FROM repository_config
			WHERE key = ?
		`, key).Scan(&value)
	default:
		return sql.NullString{}, fmt.Errorf("unsupported DB backend")
	}

	return value, err
}

func collectObservedFromBlocks(ctx context.Context, dbconn *sql.DB, compression, encryption, packing map[string]struct{}) error {
	var count int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks`).Scan(&count); err != nil {
		return fmt.Errorf("derive repository capabilities: count blocks rows: %w", err)
	}
	if count == 0 {
		return nil
	}

	addSet(packing, PackingLegacySingle)
	addSet(compression, CompressionNone)

	rows, err := dbconn.QueryContext(ctx, `SELECT DISTINCT codec FROM blocks`)
	if err != nil {
		return fmt.Errorf("derive repository capabilities: distinct blocks codec: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			return fmt.Errorf("derive repository capabilities: scan blocks codec: %w", err)
		}
		if !raw.Valid {
			continue
		}
		addSet(encryption, normalizeEncryptionCodec(raw.String))
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("derive repository capabilities: iterate blocks codec: %w", err)
	}

	return nil
}

func collectObservedFromStorageBlocks(ctx context.Context, dbconn *sql.DB, compression, encryption, packing map[string]struct{}) error {
	var count int64
	if err := dbconn.QueryRowContext(ctx, `SELECT COUNT(*) FROM storage_blocks`).Scan(&count); err != nil {
		return fmt.Errorf("derive repository capabilities: count storage_blocks rows: %w", err)
	}
	if count == 0 {
		return nil
	}

	addSet(packing, PackingPackedMulti)

	rows, err := dbconn.QueryContext(ctx, `
		SELECT DISTINCT codec, compression_codec
		FROM storage_blocks
	`)
	if err != nil {
		return fmt.Errorf("derive repository capabilities: distinct storage_blocks codecs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var encryptionRaw, compressionRaw sql.NullString
		if err := rows.Scan(&encryptionRaw, &compressionRaw); err != nil {
			return fmt.Errorf("derive repository capabilities: scan storage_blocks codecs: %w", err)
		}
		if encryptionRaw.Valid {
			addSet(encryption, normalizeEncryptionCodec(encryptionRaw.String))
		}
		if compressionRaw.Valid {
			addSet(compression, normalizeCompressionCodec(compressionRaw.String))
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("derive repository capabilities: iterate storage_blocks codecs: %w", err)
	}

	if len(compression) == 0 {
		addSet(compression, CompressionNone)
	}

	return nil
}

func normalizeEncryptionCodec(raw string) string {
	codec := strings.TrimSpace(strings.ToLower(raw))
	switch codec {
	case "plain", "none", "":
		return EncryptionNone
	case "aes-gcm":
		return EncryptionAESGCM
	default:
		return codec
	}
}

func normalizeCompressionCodec(raw string) string {
	codec := strings.TrimSpace(strings.ToLower(raw))
	if codec == "" {
		return CompressionNone
	}
	return codec
}

func sortedSet(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func addSet(m map[string]struct{}, value string) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return
	}
	m[trimmed] = struct{}{}
}

func tableExists(ctx context.Context, dbconn *sql.DB, tableName string) (bool, error) {
	switch db.BackendFromDB(dbconn) {
	case db.BackendPostgres:
		var exists bool
		err := dbconn.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)
		`, tableName).Scan(&exists)
		return exists, err
	case db.BackendSQLite:
		var count int
		err := dbconn.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM sqlite_master
			WHERE type = 'table' AND name = ?
		`, tableName).Scan(&count)
		return count > 0, err
	default:
		return false, fmt.Errorf("unsupported DB backend")
	}
}

func columnExists(ctx context.Context, dbconn *sql.DB, tableName, columnName string) (bool, error) {
	switch db.BackendFromDB(dbconn) {
	case db.BackendPostgres:
		var exists bool
		err := dbconn.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
			)
		`, tableName, columnName).Scan(&exists)
		return exists, err
	case db.BackendSQLite:
		rows, err := dbconn.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", tableName))
		if err != nil {
			return false, err
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var (
				cid      int
				name     string
				dataType string
				notNull  int
				defaultV sql.NullString
				primaryK int
			)
			if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultV, &primaryK); err != nil {
				return false, err
			}
			if strings.EqualFold(name, columnName) {
				return true, nil
			}
		}
		if err := rows.Err(); err != nil {
			return false, err
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported DB backend")
	}
}
