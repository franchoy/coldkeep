package storage

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/franchoy/coldkeep/internal/container"
	"github.com/franchoy/coldkeep/internal/db"
	_ "github.com/mattn/go-sqlite3"
)

type TestRepository struct {
	DB            *sql.DB
	Storage       StorageContext
	ContainersDir string
}

type TestRepositoryOption func(*testRepositoryConfig)

type testRepositoryConfig struct {
	compressionCodec string
	compressionLevel *int
}

func WithCompression(codec string) TestRepositoryOption {
	return func(cfg *testRepositoryConfig) {
		cfg.compressionCodec = strings.TrimSpace(codec)
	}
}

func WithCompressionLevel(level int) TestRepositoryOption {
	return func(cfg *testRepositoryConfig) {
		cfg.compressionLevel = &level
	}
}

func NewTestRepository(t *testing.T, opts ...TestRepositoryOption) *TestRepository {
	t.Helper()

	cfg := testRepositoryConfig{compressionCodec: "none"}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	dbconn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	t.Cleanup(func() { _ = dbconn.Close() })

	if err := db.RunMigrations(dbconn); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	tx, err := dbconn.Begin()
	if err != nil {
		t.Fatalf("begin repository config tx: %v", err)
	}
	if err := SetDefaultCompression(tx, cfg.compressionCodec); err != nil {
		_ = tx.Rollback()
		t.Fatalf("set repository default compression %q: %v", cfg.compressionCodec, err)
	}
	if cfg.compressionLevel != nil {
		if err := SetDefaultCompressionLevel(tx, *cfg.compressionLevel); err != nil {
			_ = tx.Rollback()
			t.Fatalf("set repository default compression level %d: %v", *cfg.compressionLevel, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit repository config tx: %v", err)
	}

	containersDir := t.TempDir()
	storageCtx := StorageContext{
		DB:           dbconn,
		Writer:       container.NewLocalWriterWithDirAndDB(containersDir, container.GetContainerMaxSize(), dbconn),
		ContainerDir: containersDir,
	}

	return &TestRepository{
		DB:            dbconn,
		Storage:       storageCtx,
		ContainersDir: containersDir,
	}
}

func RequireTestCompression(t *testing.T, codec string) {
	t.Helper()
	want := strings.ToLower(strings.TrimSpace(codec))
	if want == "" {
		want = "none"
	}
	got := strings.ToLower(strings.TrimSpace(os.Getenv("COLDKEEP_TEST_COMPRESSION")))
	if got != want {
		t.Skipf("set COLDKEEP_TEST_COMPRESSION=%s to run this test", want)
	}
}
