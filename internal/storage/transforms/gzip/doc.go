// Package gzip is a reserved stub for a gzip compression transform.
//
// # NOT implemented — stub only
//
// This directory is a placeholder. No gzip transform is implemented or wired
// into the runtime. If compression is needed, use the zstd codec via the
// canonical path in internal/blocks (GetBlockTransformer with codec "zstd").
//
// This stub exists to document intent during v1.10 engine extraction planning.
// It must not be imported by production code.
package gzip
