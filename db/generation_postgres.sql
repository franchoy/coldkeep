-- Explicit checkpointed 19 -> 20 maintenance. No capture rows are backfilled.
CREATE TABLE public.schema_upgrade_operation (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 operation_id TEXT NOT NULL UNIQUE CHECK (operation_id ~ '^upgrade_[0-9a-f]{32}$'),
 repository_id TEXT NOT NULL REFERENCES public.repository_identity(repository_id),
 root_id TEXT NOT NULL REFERENCES public.storage_root_identity(root_id),
 binding_revision BIGINT NOT NULL,
 plan_sha256 TEXT NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
 request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
 prior_plan TEXT NOT NULL CHECK (prior_plan ~ '^[0-9a-f]{64}$'),
 generation TEXT NOT NULL CHECK (generation ~ '^[0-9a-f]{32}$'),
 manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
 from_version INTEGER NOT NULL CHECK (from_version = 19),
 to_version INTEGER NOT NULL CHECK (to_version = 20),
 FOREIGN KEY (root_id,binding_revision) REFERENCES public.root_binding_revision(root_id,revision)
);
CREATE TABLE public.generation_policy (
 policy_id TEXT PRIMARY KEY CHECK (policy_id ~ '^policy_[0-9a-f]{32}$'),
 repository_id TEXT NOT NULL REFERENCES public.repository_identity(repository_id),
 source_id TEXT NOT NULL REFERENCES public.configured_source(source_id),
 descriptor BYTEA NOT NULL CHECK (octet_length(descriptor) BETWEEN 2 AND 4096),
 descriptor_sha256 TEXT NOT NULL CHECK (descriptor_sha256 = encode(sha256(descriptor),'hex')),
 UNIQUE (source_id,policy_id,descriptor_sha256)
);
CREATE TABLE public.capture_generation (
 generation_id TEXT PRIMARY KEY CHECK (generation_id ~ '^generation_[0-9a-f]{32}$'),
 repository_id TEXT NOT NULL REFERENCES public.repository_identity(repository_id),
 source_id TEXT NOT NULL,
 source_revision BIGINT NOT NULL,
 policy_id TEXT NOT NULL,
 policy_sha256 TEXT NOT NULL,
 started_ns BIGINT CHECK (started_ns >= 0),
 ended_ns BIGINT CHECK (ended_ns >= 0),
 scan_state TEXT NOT NULL CHECK (scan_state IN ('unknown','pending','partial','enumerated')),
 outcome TEXT NOT NULL CHECK (outcome IN ('unknown','incomplete','failed')),
 publication_receipt TEXT CHECK (publication_receipt IS NULL),
 CHECK (ended_ns IS NULL OR (started_ns IS NOT NULL AND ended_ns >= started_ns)),
 FOREIGN KEY (source_id,source_revision) REFERENCES public.source_binding_revision(source_id,revision),
 FOREIGN KEY (source_id,policy_id,policy_sha256) REFERENCES public.generation_policy(source_id,policy_id,descriptor_sha256)
);
CREATE TABLE public.generation_root (
 generation_id TEXT PRIMARY KEY REFERENCES public.capture_generation(generation_id),
 mode INTEGER CHECK (mode BETWEEN 0 AND 4095),
 mtime_ns BIGINT
);
CREATE TABLE public.generation_entry (
 generation_id TEXT NOT NULL REFERENCES public.capture_generation(generation_id),
 entry_id TEXT NOT NULL CHECK (entry_id ~ '^entry_[0-9a-f]{32}$'),
 parent_id TEXT,
 parent_kind TEXT NOT NULL CHECK (parent_kind = 'directory'),
 component BYTEA NOT NULL CHECK (octet_length(component) BETWEEN 1 AND 255) CHECK (position(decode('00','hex') IN component)=0) CHECK (position(decode('2f','hex') IN component)=0) CHECK (component NOT IN (decode('2e','hex'),decode('2e2e','hex'))),
 kind TEXT NOT NULL CHECK (kind IN ('file','directory')),
 outcome TEXT NOT NULL CHECK (outcome IN ('stored','excluded','missing','unreadable','unstable','unsupported')),
 logical_file_id BIGINT REFERENCES public.logical_file(id),
 mode INTEGER CHECK (mode BETWEEN 0 AND 4095),
 mtime_ns BIGINT,
 PRIMARY KEY (generation_id,entry_id),
 UNIQUE (generation_id,entry_id,kind),
 UNIQUE NULLS NOT DISTINCT (generation_id,parent_id,component),
 FOREIGN KEY (generation_id,parent_id,parent_kind) REFERENCES public.generation_entry(generation_id,entry_id,kind),
 CHECK ((logical_file_id IS NOT NULL) = (kind='file' AND outcome='stored'))
);
