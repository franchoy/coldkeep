-- Explicit checkpoint-protected 18 -> 19 maintenance only. Historical identity
-- tables/constraints and the ordinary schema17 writer ceiling stay unchanged.
CREATE TABLE public.binding_operation (
 operation_id TEXT PRIMARY KEY CHECK (operation_id ~ '^bind_[0-9a-f]{32}$'),
 plan_sha256 TEXT NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
 request_sha256 TEXT NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
 root_id TEXT NOT NULL REFERENCES public.storage_root_identity(root_id),
 action TEXT NOT NULL CHECK (action IN ('bind','relocate','rebind','source-register','source-rebind','source-disable')),
 from_revision BIGINT NOT NULL CHECK (from_revision >= 0),
 to_revision BIGINT NOT NULL CHECK (to_revision = from_revision + 1),
 generation TEXT NOT NULL CHECK (generation ~ '^[0-9a-f]{32}$'),
 manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
 UNIQUE (root_id,to_revision)
);
CREATE TABLE public.root_binding_revision (
 root_id TEXT NOT NULL REFERENCES public.storage_root_identity(root_id),
 revision BIGINT NOT NULL CHECK (revision > 0),
 installation_id TEXT NOT NULL CHECK (installation_id ~ '^install_[0-9a-f]{32}$'),
 incarnation TEXT NOT NULL CHECK (incarnation ~ '^incarnation_[0-9a-f]{32}$'),
 path_base64 TEXT NOT NULL CHECK (octet_length(path_base64) BETWEEN 4 AND 4096),
 evidence TEXT NOT NULL CHECK (octet_length(evidence) BETWEEN 2 AND 4096),
 operation_id TEXT NOT NULL UNIQUE REFERENCES public.binding_operation(operation_id),
 PRIMARY KEY (root_id,revision)
);
CREATE TABLE public.root_binding_head (
 root_id TEXT PRIMARY KEY REFERENCES public.storage_root_identity(root_id),
 revision BIGINT NOT NULL,
 capability TEXT NOT NULL CHECK (capability = 'physical-binding-v1/recovery-only'),
 FOREIGN KEY (root_id,revision) REFERENCES public.root_binding_revision(root_id,revision)
);
CREATE TABLE public.configured_source (
 source_id TEXT PRIMARY KEY CHECK (source_id ~ '^source_[0-9a-f]{32}$'),
 repository_id TEXT NOT NULL REFERENCES public.repository_identity(repository_id),
 slot INTEGER NOT NULL UNIQUE CHECK (slot BETWEEN 1 AND 64),
 capture_enabled BOOLEAN NOT NULL CHECK (capture_enabled = false),
 disabled BOOLEAN NOT NULL
);
CREATE TABLE public.source_binding_revision (
 source_id TEXT NOT NULL REFERENCES public.configured_source(source_id),
 revision BIGINT NOT NULL CHECK (revision > 0),
 path_base64 TEXT NOT NULL CHECK (octet_length(path_base64) BETWEEN 4 AND 4096),
 evidence TEXT NOT NULL CHECK (octet_length(evidence) BETWEEN 2 AND 4096),
 continuity TEXT NOT NULL CHECK (continuity IN ('new','same-object','changed','disabled')),
 fresh_scan BOOLEAN NOT NULL,
 operation_id TEXT NOT NULL UNIQUE REFERENCES public.binding_operation(operation_id),
 PRIMARY KEY (source_id,revision)
);
CREATE TABLE public.source_binding_head (
 source_id TEXT PRIMARY KEY REFERENCES public.configured_source(source_id),
 revision BIGINT NOT NULL,
 FOREIGN KEY (source_id,revision) REFERENCES public.source_binding_revision(source_id,revision)
);
