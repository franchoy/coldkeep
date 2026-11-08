# Selected native feasibility and dependency decision

This matrix records an engineering pilot; it grants no product/native support.
Final clean-checkpoint outcomes and artifacts belong to [the result](phase2-result.md).

| Target or behavior | Disposition | Evidence/remaining qualification |
|---|---|---|
| Linux x86-64, local filesystem, CGO SQLite | Selected; development persistence observed | Real LocalWriter store, finalize/close/reopen and engine restore with fixed independent hash. Final checkpoint reruns required. |
| Copied historical reader | Selected; development observed | Existing old-writer fixture bytes including nonempty, empty and encrypted; unchanged 23-case oracle required. |
| CGO-disabled SQLite | Expected refusal | `sqlite_cgo_required`, no backend fallback. |
| PostgreSQL Docker / embedded UI | Selected; development observed | Separate non-root proof profile, retained independent CLI image, actual mount/exposure/browser evidence. |
| Full SQLite lifecycle/default, live GC | Later support qualification | Live GC remains refused; durability, upgrade, capacity, native distribution and maintenance unqualified. |
| Windows / macOS / other architectures | Not selected; not attempted | No runnable selected-target qualification or maintenance capacity in this Linux run. No cross-build implies support. |
| Installers, signing/notarization, WebView, OS startup | Not attempted | Later independently selected delivery actions; no new resource purchase required. |
| External physical LAN/IPv6 peer | Not attempted | Host and isolated Docker network probes are recorded; external peer behavior remains unqualified. |
| Physical failure-domain independence | Unknown | Same-host paths/containers cannot establish host/disk-loss independence. |

The sole Go dependency change is mattn/go-sqlite3 v1.14.24 → v1.14.53 and its
necessary sums. Upstream's current [release](https://github.com/mattn/go-sqlite3/releases/tag/v1.14.53)
maintains the same driver line, updates its embedded SQLite and includes connection
lifetime corrections. [Build requirements](https://github.com/mattn/go-sqlite3/blob/master/README.md)
still require CGO and a C compiler. The selection is qualified by fresh affected
SQLite evidence; being newest alone is not qualification. Language floor 1.25,
Go 1.26.10, pq 1.11.2, compress 1.20.2 and x/sys 0.44.0 remain fixed. The development
run observed SQLite 3.54.1 and GCC 13.3.0; the final report records actual runtime,
linker/interpreter/library/filesystem facts and exact source binding.

Frontend choices were checked against [React versions](https://react.dev/versions),
[Vite production builds](https://vite.dev/guide/build),
[TypeScript releases](https://github.com/microsoft/TypeScript/releases), official
npm publisher manifests and [Node releases](https://nodejs.org/en/about/previous-releases).
Node 24 LTS is a private build/test prerequisite; system Node 18 was left unchanged.
Actual resolved versions, licenses and dependency hashes are retained with the lockfile.

Docker's [port-publication documentation](https://docs.docker.com/engine/network/port-publishing/)
and [Engine 28 notes](https://docs.docker.com/engine/release-notes/28/) describe the
pre-28 localhost/L2 issue. The runner refuses engines older than 28, records actual
engine/network settings and tests selected IPv4/IPv6 host and isolated-network
exposure. Version alone is insufficient. [Compose long binds](https://docs.docker.com/reference/compose-file/services/#volumes)
allow refusing missing host paths; the actual missing-mount test is blocking.
No global Docker/firewall changes or host upgrades are part of this proof.
