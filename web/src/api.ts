// Generated from proof-openapi.json; do not edit.
export type Problem = { code: string; detail: string };
export type BootstrapRequest = { secret: string };
export type SessionResult = { token: string; expires_seconds: number };
export type HistoryEntry = { id: string; name: string; size: string };
export type HistoryResult = { entries: Array<HistoryEntry>; next_cursor: string };
export type RestoreRequest = { entry_id: string };
export type RestoreResult = { outcome: string; code: string; detail: string };
export type Capabilities = { history: boolean; restore: boolean; search: boolean; jobs: boolean; live_gc: boolean; durable_acceptance: boolean };
export type Status = { application: string; api: string; profile: string; backend: string; destination: string; source_revision: string; source_tree: string; source_dirty: boolean; asset_digest: string };
