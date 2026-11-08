package recoveryexport

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type tableSpec struct{ name, columns, variable, types, required string }

// Exact schema-17 tables, including repair/retirement references. Unknown table
// or column layouts cannot silently become a projected catalog.
var catalogTables = []tableSpec{
	{"schema_version", "catalog_version", "", "int", "catalog_version"},
	{"container", "id filename sealed sealing container_hash quarantine current_size max_size created_at updated_at", "filename container_hash", "int text bool bool text bool int int time time", "id filename sealed sealing quarantine current_size max_size created_at updated_at"},
	{"chunk", "id chunk_hash size status live_ref_count pin_count retry_count created_at updated_at chunker_version", "chunk_hash status chunker_version", "int text int text int int int time time text", "id chunk_hash size status live_ref_count pin_count retry_count created_at updated_at chunker_version"},
	{"logical_file", "id original_name total_size file_hash ref_count chunker_version status retry_count created_at updated_at", "original_name file_hash chunker_version status", "int text int text int text text int time time", "id original_name total_size file_hash ref_count chunker_version status retry_count created_at updated_at"},
	{"physical_file", "path logical_file_id mode mtime uid gid is_metadata_complete", "path", "text int int time int int bool", "path logical_file_id is_metadata_complete"},
	{"file_chunk", "logical_file_id chunk_id chunk_order created_at", "", "int int int time", "logical_file_id chunk_id chunk_order created_at"},
	{"repository_config", "key value", "key value", "text text", "key value"},
	{"blocks", "id chunk_id codec format_version plaintext_size stored_size nonce container_id block_offset created_at updated_at", "codec nonce", "int int text int int int bytea int int time time", "id chunk_id codec format_version plaintext_size stored_size container_id block_offset created_at updated_at"},
	{"storage_blocks", "id format_version codec plaintext_size compression_codec compression_level compressed_size stored_size container_id container_offset block_hash compression_ratio payload_hash compressed_hash physical_hash created_at", "codec compression_codec block_hash payload_hash compressed_hash physical_hash", "int int text int text int int int int int bytea float text bytea bytea time", "id format_version codec plaintext_size compression_codec stored_size container_id container_offset block_hash created_at"},
	{"chunk_block_refs", "chunk_id block_id offset_in_block size_in_block", "", "int int int int", "chunk_id block_id offset_in_block size_in_block"},
	{"snapshot", "id created_at type label parent_id", "id type label parent_id", "text time text text text", "id created_at type"},
	{"snapshot_path", "id path", "path", "int text", "id path"},
	{"snapshot_file", "id snapshot_id path_id logical_file_id size mode mtime", "snapshot_id", "int text int int int int time", "id snapshot_id path_id logical_file_id"},
	{"store_repair_attempt", "id logical_file_id source_file_hash source_total_size recipe_fingerprint status created_at updated_at", "source_file_hash recipe_fingerprint status", "int int text int text text time time", "id logical_file_id source_file_hash source_total_size recipe_fingerprint status created_at updated_at"},
	{"store_repair_container", "attempt_id container_id physical_size physical_hash status", "physical_hash status", "int int int text text", "attempt_id container_id physical_size physical_hash status"},
	{"store_repair_block", "attempt_id block_ordinal format_version codec plaintext_size compression_codec compression_level compressed_size stored_size container_id container_offset block_hash compression_ratio payload_hash compressed_hash physical_hash legacy_nonce", "codec compression_codec block_hash payload_hash compressed_hash physical_hash legacy_nonce", "int int int text int text int int int int int bytea float text bytea bytea bytea", "attempt_id block_ordinal format_version codec plaintext_size compression_codec compressed_size stored_size container_id container_offset block_hash compression_ratio payload_hash compressed_hash physical_hash"},
	{"store_repair_chunk", "attempt_id chunk_id chunk_order block_ordinal offset_in_block size_in_block", "", "int int int int int int", "attempt_id chunk_id chunk_order block_ordinal offset_in_block size_in_block"},
	{"retired_chunk_block_ref", "block_id embedded_chunk_id offset_in_block size_in_block repair_attempt_id retired_at", "", "int int int int int time", "block_id embedded_chunk_id offset_in_block size_in_block repair_attempt_id retired_at"},
	{"retired_legacy_block_extent", "container_id block_offset stored_size plaintext_size codec format_version nonce historical_block_id historical_chunk_id repair_attempt_id retired_at", "codec nonce", "int int int int text int bytea int int int time", "container_id block_offset stored_size plaintext_size codec format_version historical_block_id historical_chunk_id repair_attempt_id retired_at"},
}

func readCatalog(ctx context.Context, tx *sql.Tx, inv *Inventory, h *hooks) error {
	// LIMIT bounds enumeration even with a damaged/unexpected catalog. The
	// information schema query also rejects foreign tables and views as readers.
	rows, err := tx.QueryContext(ctx, `SELECT c.relname,a.attname,t.typname,c.relkind
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
 JOIN pg_type t ON t.oid=a.atttypid
 WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f')
 ORDER BY c.relname,a.attnum LIMIT 513`)
	if err != nil {
		return fmt.Errorf("%w: schema inspection", ErrUnsupported)
	}
	shape := map[string][]string{}
	types := map[string][]string{}
	n := 0
	for rows.Next() {
		var table, col, typ, kind string
		if err = rows.Scan(&table, &col, &typ, &kind); err != nil {
			break
		}
		n++
		if n > 512 {
			err = ErrLimit
			break
		}
		if kind != "r" || !strings.Contains("|int4|int8|text|bytea|bool|float4|float8|timestamp|timestamptz|", "|"+typ+"|") {
			err = ErrUnsupported
			break
		}
		shape[table] = append(shape[table], col)
		types[table] = append(types[table], typ)
	}
	err = joinRows(err, rows)
	if err != nil {
		return err
	}
	if len(shape) != len(catalogTables) {
		return fmt.Errorf("%w: schema table set", ErrUnsupported)
	}
	for _, spec := range catalogTables {
		if strings.Join(shape[spec.name], " ") != spec.columns {
			return fmt.Errorf("%w: columns for %s", ErrUnsupported, spec.name)
		}
		for k, want := range strings.Fields(spec.types) {
			got := types[spec.name][k]
			allowed := map[string]string{"int": "|int4|int8|", "time": "|timestamp|timestamptz|", "float": "|float4|float8|", "text": "|text|", "bytea": "|bytea|", "bool": "|bool|"}
			if !strings.Contains(allowed[want], "|"+got+"|") {
				return fmt.Errorf("%w: column types for %s", ErrUnsupported, spec.name)
			}
		}
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT catalog_version FROM public.schema_version LIMIT 1`).Scan(&version); err != nil || version != 17 {
		return fmt.Errorf("%w: schema version", ErrUnsupported)
	}
	for _, spec := range catalogTables {
		if err = ctx.Err(); err != nil {
			return err
		}
		// Every unbounded value is checked on the server before row serialization;
		// a large TOASTed value cannot cause an unbounded client-side allocation.
		conditions := []string{"false"}
		for _, col := range strings.Fields(spec.variable) {
			conditions = append(conditions, "octet_length("+pqIdent(col)+")>4096")
		}
		q := `SELECT CASE WHEN ` + strings.Join(conditions, " OR ") + ` THEN NULL ELSE row_to_json(t)::text END FROM public.` + pqIdent(spec.name) + ` t LIMIT $1`
		rows, err = tx.QueryContext(ctx, q, inv.Limits.Rows-inv.Rows+1)
		if err != nil {
			return fmt.Errorf("%w: query %s", ErrCorrupt, spec.name)
		}
		for rows.Next() {
			var raw sql.NullString
			if err = rows.Scan(&raw); err != nil {
				break
			}
			if !raw.Valid || inv.Rows >= inv.Limits.Rows || int64(len(raw.String)) > inv.Limits.CatalogBytes-inv.CatalogBytes {
				err = ErrLimit
				break
			}
			inv.Rows++
			inv.CatalogBytes += int64(len(raw.String))
			var row Row
			d := json.NewDecoder(strings.NewReader(raw.String))
			d.UseNumber()
			if err = d.Decode(&row); err != nil {
				break
			}
			for _, col := range strings.Fields(spec.required) {
				if row[col] == nil {
					err = graphError(spec.name + " required field")
					break
				}
			}
			if err != nil {
				break
			}
			inv.Catalog[spec.name] = append(inv.Catalog[spec.name], row)
			if err = h.at("row:"+spec.name, tx); err != nil {
				break
			}
			if err = ctx.Err(); err != nil {
				break
			}
		}
		err = joinRows(err, rows)
		if err != nil {
			return err
		}
	}
	if len(inv.Catalog["schema_version"]) != 1 {
		return fmt.Errorf("%w: schema singleton", ErrUnsupported)
	}
	return nil
}
func pqIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func joinRows(err error, rows *sql.Rows) error {
	if err == nil {
		err = rows.Err()
	}
	e := rows.Close()
	if err == nil {
		err = e
	}
	return err
}
func (r Row) n(k string) int64 {
	v, ok := r[k].(json.Number)
	if !ok {
		return -1
	}
	n, e := v.Int64()
	if e != nil {
		return -1
	}
	return n
}
func (r Row) s(k string) string { v, _ := r[k].(string); return v }
func (r Row) b(k string) bool   { v, _ := r[k].(bool); return v }
func (r Row) blob(k string) []byte {
	v := r.s(k)
	if !strings.HasPrefix(v, `\x`) {
		return nil
	}
	b, _ := hex.DecodeString(v[2:])
	return b
}
func validHash(s string) bool       { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
func graphError(table string) error { return fmt.Errorf("%w: %s graph/metadata", ErrCorrupt, table) }
func index(rows []Row, key string) (map[int64]Row, error) {
	m := map[int64]Row{}
	for _, r := range rows {
		n := r.n(key)
		if n <= 0 || m[n] != nil {
			return nil, graphError(key)
		}
		m[n] = r
	}
	return m, nil
}
func strIndex(rows []Row, key string) (map[string]Row, error) {
	m := map[string]Row{}
	for _, r := range rows {
		s := r.s(key)
		if s == "" || m[s] != nil {
			return nil, graphError(key)
		}
		m[s] = r
	}
	return m, nil
}

func validateGraph(ctx context.Context, i *Inventory) error {
	c := i.Catalog
	files, e := index(c["logical_file"], "id")
	if e != nil {
		return e
	}
	chunks, e := index(c["chunk"], "id")
	if e != nil {
		return e
	}
	containers, e := index(c["container"], "id")
	if e != nil {
		return e
	}
	packed, e := index(c["storage_blocks"], "id")
	if e != nil {
		return e
	}
	refs, e := index(c["chunk_block_refs"], "chunk_id")
	if e != nil {
		return e
	}
	legacy, e := index(c["blocks"], "chunk_id")
	if e != nil {
		return e
	}
	snapshots, e := strIndex(c["snapshot"], "id")
	if e != nil {
		return e
	}
	paths, e := index(c["snapshot_path"], "id")
	if e != nil {
		return e
	}
	attempts, e := index(c["store_repair_attempt"], "id")
	if e != nil {
		return e
	}
	for _, r := range c["container"] {
		if r.b("sealing") || r.b("quarantine") || r.n("max_size") <= 64 || r.n("current_size") < 64 {
			return graphError("container/interrupted")
		}
		if r["container_hash"] != nil && !validHash(r.s("container_hash")) {
			return graphError("container hash")
		}
	}
	for _, r := range files {
		if r.s("status") != "COMPLETED" || r.n("total_size") < 0 || r.s("chunker_version") == "" || !validHash(r.s("file_hash")) {
			return graphError("logical_file/interrupted")
		}
	}
	for _, r := range chunks {
		if r.s("status") != "COMPLETED" || r.n("size") <= 0 || r.n("pin_count") < 0 || r.n("live_ref_count") < 0 || r.s("chunker_version") == "" || !validHash(r.s("chunk_hash")) {
			return graphError("chunk/interrupted")
		}
	}
	physicalCounts := map[int64]int64{}
	for _, r := range c["physical_file"] {
		physicalCounts[r.n("logical_file_id")]++
		if r.s("path") == "" || files[r.n("logical_file_id")] == nil {
			return fmt.Errorf("%w: physical_file references logical_file id=%d or has empty path", ErrCorrupt, r.n("logical_file_id"))
		}
	}
	for id, r := range files {
		if r.n("ref_count") != physicalCounts[id] {
			return graphError("logical ref_count")
		}
	}
	for _, r := range snapshots {
		if r.s("type") != "full" && r.s("type") != "partial" {
			return graphError("snapshot")
		}
		if p := r.s("parent_id"); p != "" && snapshots[p] == nil {
			return graphError("snapshot parent")
		}
	}
	// Cycle detection has linear work, rather than walking each ancestry anew.
	colors := map[string]int{}
	for id := range snapshots {
		trail := []string{}
		p := id
		for p != "" && colors[p] == 0 {
			colors[p] = 1
			trail = append(trail, p)
			p = snapshots[p].s("parent_id")
		}
		if p != "" && colors[p] == 1 {
			return graphError("snapshot cycle")
		}
		for _, p := range trail {
			colors[p] = 2
		}
	}
	membership := map[string]bool{}
	for _, r := range c["snapshot_file"] {
		f := files[r.n("logical_file_id")]
		p := paths[r.n("path_id")]
		if f == nil || p == nil || p.s("path") == "" || snapshots[r.s("snapshot_id")] == nil {
			return graphError("snapshot_file")
		}
		if r["size"] != nil && r.n("size") != f.n("total_size") {
			return graphError("snapshot size")
		}
		k := fmt.Sprint(r.s("snapshot_id"), "/", r.n("path_id"))
		if membership[k] {
			return graphError("snapshot duplicate")
		}
		membership[k] = true
	}
	recipes := map[int64][]Row{}
	chunkCounts := map[int64]int64{}
	for _, r := range c["file_chunk"] {
		if files[r.n("logical_file_id")] == nil || chunks[r.n("chunk_id")] == nil || r.n("chunk_order") < 0 {
			return graphError("file_chunk")
		}
		if physicalCounts[r.n("logical_file_id")] > 0 {
			chunkCounts[r.n("chunk_id")]++
		}
		recipes[r.n("logical_file_id")] = append(recipes[r.n("logical_file_id")], r)
	}
	for id, r := range chunks {
		if r.n("live_ref_count") != chunkCounts[id] {
			return graphError("chunk live_ref_count")
		}
	}
	for id, f := range files {
		rows := recipes[id]
		sort.Slice(rows, func(a, b int) bool { return rows[a].n("chunk_order") < rows[b].n("chunk_order") })
		var size int64
		for order, r := range rows {
			n := chunks[r.n("chunk_id")].n("size")
			if r.n("chunk_order") != int64(order) || n > f.n("total_size")-size {
				return graphError("recipe")
			}
			size += n
		}
		if size != f.n("total_size") {
			return graphError("recipe size")
		}
	}
	for id := range chunks {
		if refs[id] == nil && legacy[id] == nil {
			return fmt.Errorf("%w: chunk id=%d has no placement", ErrCorrupt, id)
		}
	}
	for _, r := range c["chunk_block_refs"] {
		if chunks[r.n("chunk_id")] == nil || packed[r.n("block_id")] == nil || r.n("offset_in_block") < 0 || r.n("size_in_block") != chunks[r.n("chunk_id")].n("size") {
			return graphError("chunk_block_refs")
		}
	}
	for _, table := range []string{"blocks", "storage_blocks", "retired_legacy_block_extent", "store_repair_block"} {
		for _, r := range c[table] {
			if r.n("format_version") != 1 {
				return fmt.Errorf("%w: %s id=%d container_id=%d format_version=%d", ErrUnsupported, table, r.n("id"), r.n("container_id"), r.n("format_version"))
			}
			if containers[r.n("container_id")] == nil || r.n("plaintext_size") <= 0 || r.n("stored_size") <= 0 {
				return graphError(table)
			}
			if table == "blocks" || table == "retired_legacy_block_extent" {
				if r.s("codec") != "plain" && r.s("codec") != "aes-gcm" {
					return ErrUnsupported
				}
				if r.n("block_offset") < 64 {
					return graphError(table)
				}
				if table == "blocks" && (chunks[r.n("chunk_id")] == nil || r.n("plaintext_size") != chunks[r.n("chunk_id")].n("size")) {
					return graphError(table)
				}
			} else {
				if r.s("codec") != "none" && r.s("codec") != "aes-gcm" {
					return ErrUnsupported
				}
				if r.n("container_offset") < 64 || len(r.blob("block_hash")) != 32 {
					return graphError(table)
				}
				switch r.s("compression_codec") {
				case "none":
					if r["compression_level"] != nil {
						return graphError(table)
					}
				case "zstd":
					if r.n("compression_level") < 1 || r.n("compression_level") > 9 {
						return graphError(table)
					}
				default:
					return ErrUnsupported
				}
				for _, k := range []string{"physical_hash", "compressed_hash"} {
					if r[k] != nil && len(r.blob(k)) != 32 {
						return graphError(table)
					}
				}
			}
		}
	}
	for _, r := range c["store_repair_attempt"] {
		f := files[r.n("logical_file_id")]
		if f == nil || r.s("status") != "PUBLISHED" || r.s("source_file_hash") != f.s("file_hash") || r.n("source_total_size") != f.n("total_size") || !validHash(r.s("recipe_fingerprint")) {
			return graphError("repair/interrupted")
		}
	}
	repairBlocks := map[string]Row{}
	for _, r := range c["store_repair_block"] {
		if attempts[r.n("attempt_id")] == nil || r.n("block_ordinal") < 0 {
			return graphError("repair block")
		}
		k := fmt.Sprint(r.n("attempt_id"), "/", r.n("block_ordinal"))
		if repairBlocks[k] != nil {
			return graphError("repair duplicate")
		}
		repairBlocks[k] = r
	}
	for _, r := range c["store_repair_chunk"] {
		if attempts[r.n("attempt_id")] == nil || chunks[r.n("chunk_id")] == nil || repairBlocks[fmt.Sprint(r.n("attempt_id"), "/", r.n("block_ordinal"))] == nil || r.n("chunk_order") < 0 || r.n("offset_in_block") < 0 || r.n("size_in_block") != chunks[r.n("chunk_id")].n("size") {
			return graphError("repair chunk")
		}
	}
	for _, r := range c["store_repair_container"] {
		if attempts[r.n("attempt_id")] == nil || containers[r.n("container_id")] == nil || r.s("status") != "PUBLISHED" || r.n("physical_size") < 64 || !validHash(r.s("physical_hash")) {
			return graphError("repair container")
		}
	}
	for _, r := range c["retired_chunk_block_ref"] {
		if packed[r.n("block_id")] == nil || attempts[r.n("repair_attempt_id")] == nil || r.n("embedded_chunk_id") <= 0 || r.n("offset_in_block") < 0 || r.n("size_in_block") <= 0 {
			return graphError("retired packed ref")
		}
	}
	for _, r := range c["retired_legacy_block_extent"] {
		if attempts[r.n("repair_attempt_id")] == nil {
			return graphError("retired legacy ref")
		}
	}
	// Config keys are write defaults, not new key/repository identities.
	for _, r := range c["repository_config"] {
		switch r.s("key") {
		case "default_chunker", "compression", "compression_level":
		default:
			return fmt.Errorf("%w: repository config key", ErrUnsupported)
		}
	}
	return ctx.Err()
}

func equalDigest(payload, expected []byte) bool {
	return len(expected) == 32 && bytes.Equal(payload, expected)
}
