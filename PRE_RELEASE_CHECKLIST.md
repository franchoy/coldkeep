# Pre-release Checklist

Use this checklist before cutting a release tag.

## 1) Run smoke

```bash
docker compose up -d postgres

export COLDKEEP_TEST_DB=1
export COLDKEEP_CODEC=plain
export DB_HOST=127.0.0.1
export DB_PORT=5432
export DB_USER=coldkeep
export DB_PASSWORD=coldkeep
export DB_NAME=coldkeep

go build -o coldkeep ./cmd/coldkeep
scripts/smoke.sh
```

## 2) Run integration suite

```bash
go test ./tests -count=1 -v -timeout 20m
```

## 3) Run doctor

```bash
./coldkeep doctor
./coldkeep doctor --output json
```

Expected: both succeed and JSON output is machine-readable.

## 4) Validate guarantee matrix

```bash
scripts/validate_validation_matrix.sh
```

Expected: required v1.0 guarantee rows and exit criteria are present in `VALIDATION_MATRIX.md`.

## 5) Test bootstrap on and off

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

## 6) Test clean install path

From a clean machine/container flow:

```bash
docker compose down -v
docker compose up -d postgres
docker compose build
docker compose run --rm app stats
docker compose run --rm app doctor
```

Expected: no manual local state is required beyond documented setup, and basic commands succeed.

## 7) PostgreSQL assumptions note

Operator expectation surface for supported PostgreSQL deployments:

- Schema/bootstrap: coldkeep expects the tracked schema/migration version managed by this release. With `COLDKEEP_DB_AUTO_BOOTSTRAP=true`, it may create/validate required schema objects; with bootstrap disabled, missing schema should fail fast.
- Locking behavior: coldkeep expects normal PostgreSQL row/table lock semantics and transactional guarantees under default supported isolation behavior.
- Advisory locks: maintenance and coordination flows rely on PostgreSQL advisory locking primitives being available and functioning correctly.

## Sign-off

- [ ] Smoke passed
- [ ] Integration suite passed
- [ ] Doctor checks passed
- [ ] Validation matrix audit passed
- [ ] Bootstrap on/off behavior verified
- [ ] Clean install path verified
