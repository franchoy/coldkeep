-- Explicit adoption only. Never included in ordinary bootstrap/migrations.
CREATE TABLE public.repository_identity (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 repository_id text NOT NULL UNIQUE CHECK (repository_id ~ '^repo_[0-9a-f]{32}$'),
 capability text NOT NULL CHECK (capability='identity-lineage-v1/unbound'),
 origin text NOT NULL CHECK (origin='legacy-schema17')
);
CREATE TABLE public.storage_root_identity (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 root_id text NOT NULL UNIQUE CHECK (root_id ~ '^root_[0-9a-f]{32}$'),
 repository_id text NOT NULL UNIQUE REFERENCES public.repository_identity(repository_id),
 binding text NOT NULL CHECK (binding='unbound')
);
CREATE TABLE public.device_identity (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 device_id text NOT NULL UNIQUE CHECK (device_id ~ '^device_[0-9a-f]{32}$'),
 repository_id text NOT NULL UNIQUE REFERENCES public.repository_identity(repository_id),
 provenance text NOT NULL CHECK (provenance='adopter-not-historical-capturer'),
 binding text NOT NULL CHECK (binding='unbound')
);
CREATE TABLE public.source_identity (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 source_id text NOT NULL UNIQUE CHECK (source_id ~ '^source_[0-9a-f]{32}$'),
 repository_id text NOT NULL UNIQUE REFERENCES public.repository_identity(repository_id),
 provenance text NOT NULL CHECK (provenance='synthetic-unknown-legacy-scope'),
 enabled boolean NOT NULL CHECK (enabled=false),
 binding text NOT NULL CHECK (binding='unbound')
);
CREATE TABLE public.adoption_operation (
 singleton integer PRIMARY KEY CHECK (singleton=1),
 operation_id text NOT NULL UNIQUE CHECK (operation_id ~ '^adopt_[0-9a-f]{32}$'),
 plan_sha256 text NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
 repository_id text NOT NULL UNIQUE REFERENCES public.repository_identity(repository_id),
 root_id text NOT NULL UNIQUE REFERENCES public.storage_root_identity(root_id),
 device_id text NOT NULL UNIQUE REFERENCES public.device_identity(device_id),
 source_id text NOT NULL UNIQUE REFERENCES public.source_identity(source_id),
 from_version integer NOT NULL CHECK (from_version=17),
 to_version integer NOT NULL CHECK (to_version=18),
 generation text NOT NULL CHECK (generation ~ '^[0-9a-f]{32}$'),
 manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
 legacy_root_hash text NOT NULL CHECK (legacy_root_hash ~ '^[0-9a-f]{64}$'),
 status text NOT NULL CHECK (status='committed')
);
CREATE TABLE public.legacy_snapshot_origin (
 snapshot_id text PRIMARY KEY REFERENCES public.snapshot(id),
 source_id text NOT NULL REFERENCES public.source_identity(source_id),
 operation_id text NOT NULL REFERENCES public.adoption_operation(operation_id)
);
