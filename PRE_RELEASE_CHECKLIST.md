# Pre-release Checklist

Use this checklist before cutting a release tag.

## 1) Start PostgreSQL and set CI-compatible environment

```bash
docker compose up -d coldkeep_postgres

export COLDKEEP_TEST_DB=1
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep
export DB_SSLMODE=disable
export COLDKEEP_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

`COLDKEEP_CODEC` is intentionally not exported globally here because step 3
sets it per loop iteration (`plain` then `aes-gcm`).
`COLDKEEP_KEY` is only required when codec is `aes-gcm`; it is ignored by `plain`.

## 2) Run quality-equivalent checks (CI quality job parity)

Run this block from a clean working tree when possible.
If `go mod tidy && git diff --exit-code` fails while you have local edits,
that indicates uncommitted diff in your workspace, not necessarily a test failure.

```bash
bash scripts/clean_test_storage.sh

go mod tidy && git diff --exit-code

unformatted=$(gofmt -l $(git ls-files '*.go'))
if [ -n "$unformatted" ]; then
	echo "Unformatted Go files detected:"
	echo "$unformatted"
	exit 1
fi

bash -n scripts/*.sh
scripts/validate_validation_matrix.sh
golangci-lint run ./...
go vet ./...

COLDKEEP_CODEC=plain go test -race -count=1 ./cmd/... ./internal/...
COLDKEEP_CODEC=aes-gcm COLDKEEP_KEY="$COLDKEEP_KEY" go test -race -count=1 ./cmd/... ./internal/...

go build ./...
scripts/audit_ci_enforcement.sh --local-only

go build -o coldkeep ./cmd/coldkeep
```

Expected: local quality checks match CI intent and produce no diff or lint/format failures.

## 3) Run full required CI matrix locally (all gate jobs, both codecs)

```bash
for codec in plain aes-gcm; do
	echo "=== Codec: ${codec} ==="
	export COLDKEEP_CODEC="$codec"

	# integration-correctness
	go test -race -count=1 -short ./tests/integration/...

	# integration-stress
	go test -race -count=1 ./tests/integration/...

	# integration-long-run
	COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/integration/... -run 'TestStoreGCVerifyRestoreDeleteLoopStability|TestRandomizedLongRunLifecycleSoak'

	# adversarial
	COLDKEEP_LONG_RUN=1 go test -race -count=1 ./tests/adversarial/...

	# smoke
	COLDKEEP_SMOKE_RESET_DB=1 \
	COLDKEEP_SCHEMA_PATH=db/schema_postgres.sql \
	COLDKEEP_STORAGE_DIR="$PWD/.ci-storage/${codec}" \
	COLDKEEP_SMOKE_SCHEMA_MESSAGE_GATE=1 \
	PATH="$PWD:$PATH" \
	scripts/smoke.sh
done
```

Expected: this mirrors required GitHub Actions jobs (`quality`, `integration-correctness`, `integration-stress`, `integration-long-run`, `adversarial`, `smoke`) across both codecs.

## 4) Run integration umbrella suite (optional extra confidence)

```bash
go test ./tests -count=1 -v -timeout 20m
```

## 5) Run doctor

```bash
./coldkeep doctor
./coldkeep doctor --output json
```

Expected: both succeed and JSON output is machine-readable.

## 6) Validate guarantee matrix

```bash
scripts/validate_validation_matrix.sh
```

Expected: required v1.0 guarantee rows and exit criteria are present in `VALIDATION_MATRIX.md`.

## 7) Test bootstrap on and off

Bootstrap ON (clean schema bootstrap path):

```bash
export COLDKEEP_DB_AUTO_BOOTSTRAP=true
./coldkeep stats
```

Bootstrap OFF (fail-fast when schema is missing):

```bash
unset COLDKEEP_DB_AUTO_BOOTSTRAP
# Point to a fresh DB without schema and confirm command fails fast.
./coldkeep stats
```

Expected: bootstrap on creates/validates schema path successfully; bootstrap off fails fast on missing schema.

## 8) Test clean install path

From a clean machine/container flow:

```bash
docker compose down -v
docker compose up -d coldkeep_postgres
docker compose build
docker compose run --rm coldkeep stats
docker compose run --rm coldkeep doctor
```

Expected: no manual local state is required beyond documented setup, and basic commands succeed.

## 9) PostgreSQL assumptions note

Operator expectation surface for supported PostgreSQL deployments:

- Schema/bootstrap: coldkeep expects the tracked schema/migration version managed by this release. With `COLDKEEP_DB_AUTO_BOOTSTRAP=true`, it may create/validate required schema objects; with bootstrap disabled, missing schema should fail fast.
- Locking behavior: coldkeep expects normal PostgreSQL row/table lock semantics and transactional guarantees under default supported isolation behavior.
- Advisory locks: maintenance and coordination flows rely on PostgreSQL advisory locking primitives being available and functioning correctly.

## 10) Verify CLI contract stability

Run core command paths in JSON mode and validate both success and failure envelopes.

```bash
./coldkeep doctor --output json
./coldkeep verify system --standard --output json
./coldkeep verify system --invalid-level --output json
```

Confirm:

- Success output keeps the expected top-level envelope fields (`status`, `command`) and command-specific data fields
- Error output keeps the expected generic error envelope shape (`error_class`, `exit_code`, `message`)
- Exit codes remain stable per v1.0 contract (`0` success, `2` usage, `1` general, `3` verify, `4` recovery)

Expected: no drift in CLI JSON structure, error classification, or frozen exit-code mapping.

## Sign-off

- [ ] Quality parity checks passed
- [ ] Full local CI matrix simulation passed (both codecs)
- [ ] Smoke passed
- [ ] Integration suite passed
- [ ] Doctor checks passed
- [ ] Validation matrix audit passed
- [ ] Bootstrap on/off behavior verified
- [ ] Clean install path verified
- [ ] CLI contract stability verified
