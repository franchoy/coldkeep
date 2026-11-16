package recoveryexport

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/franchoy/coldkeep/v2/internal/blocks"
	"github.com/franchoy/coldkeep/v2/internal/container"
	"github.com/klauspost/compress/zstd"
)

type payloadTask struct {
	table string
	row   Row
}

func inspectPayloads(ctx context.Context, r resolved, i *Inventory, h *hooks) (err error) {
	root, e := os.OpenRoot(r.identity.CanonicalPath)
	if e != nil {
		return graphError("payload root")
	}
	defer func() { err = errors.Join(err, root.Close(), h.at("payload-root-closed", nil)) }()
	if err = h.at("payload-root-opened", nil); err != nil {
		return
	}
	c := i.Catalog
	tasks := map[int64][]payloadTask{}
	refs, _ := index(c["chunk_block_refs"], "chunk_id")
	chunks, _ := index(c["chunk"], "id")
	packed, _ := index(c["storage_blocks"], "id")
	active := map[int64]map[int64]Row{}
	retired := map[int64]map[int64]Row{}
	repairRefs := map[string]map[int64]Row{}
	for _, v := range c["chunk_block_refs"] {
		b := v.n("block_id")
		if active[b] == nil {
			active[b] = map[int64]Row{}
		}
		active[b][v.n("chunk_id")] = v
	}
	for _, v := range c["retired_chunk_block_ref"] {
		b := v.n("block_id")
		if retired[b] == nil {
			retired[b] = map[int64]Row{}
		}
		if retired[b][v.n("embedded_chunk_id")] != nil {
			return graphError("retired duplicate")
		}
		retired[b][v.n("embedded_chunk_id")] = v
	}
	for _, v := range c["store_repair_chunk"] {
		k := fmt.Sprint(v.n("attempt_id"), "/", v.n("block_ordinal"))
		if repairRefs[k] == nil {
			repairRefs[k] = map[int64]Row{}
		}
		repairRefs[k][v.n("chunk_id")] = v
	}
	for _, table := range []string{"storage_blocks", "blocks", "store_repair_block", "retired_legacy_block_extent"} {
		for _, v := range c[table] {
			if table == "blocks" && refs[v.n("chunk_id")] != nil {
				// Companion rows for packed chunks are compatibility metadata, not direct
				// readable extents (notably for compressed blocks). Preserve every row and
				// container reference, but validate physical extents through storage_blocks.
				p := packed[refs[v.n("chunk_id")].n("block_id")]
				if p.n("container_id") != v.n("container_id") {
					return graphError("packed companion container")
				}
				continue
			}
			tasks[v.n("container_id")] = append(tasks[v.n("container_id")], payloadTask{table, v})
		}
	}
	names := map[string]bool{}
	objects := map[int64]Object{}
	var physical, decodedWork int64
	for _, v := range c["container"] {
		if err = ctx.Err(); err != nil {
			return
		}
		name := v.s("filename")
		if _, e = container.SafeContainerPath(r.identity.CanonicalPath, name); e != nil {
			return graphError("container name")
		}
		if names[name] {
			return graphError("duplicate container name")
		}
		names[name] = true
		info, e := root.Lstat(name)
		if e != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("%w: missing/unsafe container id=%d", ErrCorrupt, v.n("id"))
		}
		if info.Size() > i.Limits.ObjectBytes || info.Size() > i.Limits.TotalObjectBytes-physical {
			return ErrLimit
		}
		physical += info.Size()
		f, e := root.Open(name)
		if e != nil {
			return graphError("open object")
		}
		var prefix [12]byte
		if _, e = f.ReadAt(prefix[:], 0); e != nil {
			_ = f.Close()
			return graphError("container header")
		}
		if string(prefix[:8]) == container.ContainerMagic && binary.LittleEndian.Uint16(prefix[8:10]) > 1 {
			return errors.Join(fmt.Errorf("%w: container id=%d header major", ErrUnsupported, v.n("id")), f.Close())
		}
		fc, header, e := container.OpenReadOnlyContainerFile(f, v.n("max_size"))
		if e != nil {
			return fmt.Errorf("%w: container header id=%d", ErrCorrupt, v.n("id"))
		}
		obj, e := inspectObject(ctx, fc, header, v, tasks[v.n("id")], r, i, active, retired, repairRefs, chunks, &decodedWork, h)
		e = errors.Join(e, fc.Close(), h.at("object-closed", nil))
		if e != nil {
			return fmt.Errorf("container id=%d: %w", v.n("id"), e)
		}
		objects[obj.ID] = obj
		i.Objects = append(i.Objects, obj)
	}
	for _, v := range c["store_repair_container"] {
		o := objects[v.n("container_id")]
		if o.Size != v.n("physical_size") || o.SHA256 != v.s("physical_hash") {
			return graphError("repair physical object")
		}
	}
	// Bounded top-level orphan observations. No pruning, recovery, traversal of
	// unreferenced directories, or invented payloads for empty logical files.
	dir, e := root.Open(".")
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	for count := 0; ; count++ {
		if err = ctx.Err(); err != nil {
			return
		}
		entries, e := dir.ReadDir(1)
		if len(entries) == 0 && e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if count >= i.Limits.DirectoryEntries {
			return ErrLimit
		}
		name := entries[0].Name()
		if name == coordinationDirectory || names[name] {
			continue
		}
		i.Uncataloged = append(i.Uncataloged, name)
	}
	sort.Slice(i.Objects, func(a, b int) bool { return i.Objects[a].ID < i.Objects[b].ID })
	sort.Strings(i.Uncataloged)
	return nil
}

const coordinationDirectory = ".coldkeep-control"

func inspectObject(ctx context.Context, fc *container.FileContainer, header container.Header, v Row, tasks []payloadTask, r resolved, i *Inventory,
	active, retired map[int64]map[int64]Row, repairRefs map[string]map[int64]Row, chunks map[int64]Row, work *int64, h *hooks) (Object, error) {
	obj := Object{ID: v.n("id"), Name: v.s("filename"), Size: fc.Size(), RequiredEnd: 64, FormatMajor: header.FormatMajor}
	if (header.FormatMajor == 1 && header.FormatMinor != 0) || header.Flags != 0 || header.CodecID > 2 {
		return obj, ErrUnsupported
	}
	if fc.Size() != v.n("current_size") {
		return obj, graphError("container size")
	}
	digest := sha256.New()
	for offset := int64(0); offset < fc.Size(); {
		if err := ctx.Err(); err != nil {
			return obj, err
		}
		n := min(int64(64<<10), fc.Size()-offset)
		b, err := fc.ReadAt(offset, n)
		if err != nil {
			return obj, graphError("object read")
		}
		_, _ = digest.Write(b)
		offset += n
	}
	obj.SHA256 = hex.EncodeToString(digest.Sum(nil))
	if want := v.s("container_hash"); want != "" && want != obj.SHA256 {
		return obj, graphError("container digest")
	}
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return obj, err
		}
		row := task.row
		offset := row.n("container_offset")
		legacy := task.table == "blocks" || task.table == "retired_legacy_block_extent"
		if legacy {
			offset = row.n("block_offset")
		}
		n := row.n("stored_size")
		plain := row.n("plaintext_size")
		if n > i.Limits.BlockBytes || plain > i.Limits.BlockBytes || *work > 256<<20-plain {
			return obj, ErrLimit
		}
		*work += plain
		if offset < 64 || offset > fc.Size() || n > fc.Size()-offset {
			return obj, graphError("required extent")
		}
		obj.RequiredEnd = max(obj.RequiredEnd, offset+n)
		data, err := fc.ReadAt(offset, n)
		if err != nil {
			return obj, graphError("payload read")
		}
		if err = h.at("payload-read", nil); err != nil {
			return obj, err
		}
		if !legacy {
			if hash := row.blob("physical_hash"); len(hash) > 0 && !matchesHash(data, hash) {
				return obj, graphError("physical digest")
			}
		}
		if row.s("codec") == "aes-gcm" {
			i.KeyRequired = true
			i.KeyReference = r.keyRef
			if len(r.key) == 0 {
				return obj, ErrKeyUnavailable
			}
			nonce := row.blob("nonce")
			if !legacy {
				if len(data) < 28 {
					return obj, graphError("encrypted block")
				}
				nonce = data[:12]
				data = data[12:]
			}
			data, err = (&blocks.AESGCMTransformer{Key: r.key}).Decode(ctx, blocks.DecodeInput{Descriptor: blocks.Descriptor{Nonce: nonce}, Payload: data})
			if err != nil {
				return obj, graphError("payload authentication/key mismatch")
			}
		}
		if legacy {
			if int64(len(data)) != plain {
				return obj, graphError("legacy size")
			}
			if task.table == "blocks" && !matchesHexHash(data, chunks[row.n("chunk_id")].s("chunk_hash")) {
				return obj, graphError("legacy chunk digest")
			}
			continue
		}
		if row["compressed_size"] != nil && int64(len(data)) != row.n("compressed_size") {
			return obj, graphError("compressed size")
		}
		if hash := row.blob("compressed_hash"); len(hash) > 0 && !matchesHash(data, hash) {
			return obj, graphError("compressed digest")
		}
		if row.s("compression_codec") == "zstd" {
			decoder, e := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(uint64(i.Limits.BlockBytes)), zstd.WithDecodeAllCapLimit(true))
			if e != nil {
				return obj, ErrUnsupported
			}
			data, err = decoder.DecodeAll(data, make([]byte, 0, int(plain)))
			decoder.Close()
			if err != nil {
				return obj, graphError("compressed encoding")
			}
		}
		if int64(len(data)) != plain {
			return obj, graphError("decoded size")
		}
		// Inspect actual encoded requirements even when the catalog says v1.
		if len(data) < 20 {
			return obj, graphError("packed header")
		}
		if binary.LittleEndian.Uint32(data[:4]) != blocks.BlockMagicV1 || binary.LittleEndian.Uint16(data[4:6]) != 1 || binary.LittleEndian.Uint16(data[6:8]) != 0 {
			return obj, ErrUnsupported
		}
		if binary.LittleEndian.Uint32(data[8:12]) > uint32(i.Limits.Rows) {
			return obj, ErrLimit
		}
		if !matchesHash(data, row.blob("block_hash")) {
			return obj, graphError("packed logical digest")
		}
		block, e := blocks.DecodeBlock(data)
		if e != nil {
			return obj, graphError("packed encoding")
		}
		live := active[row.n("id")]
		old := retired[row.n("id")]
		if task.table == "store_repair_block" {
			live = repairRefs[fmt.Sprint(row.n("attempt_id"), "/", row.n("block_ordinal"))]
			old = nil
		}
		if len(block.Entries) != len(live)+len(old) {
			return obj, graphError("packed entry closure")
		}
		seen := map[uint64]bool{}
		for k, entry := range block.Entries {
			if seen[entry.ChunkID] || entry.ChunkID > 1<<63-1 {
				return obj, graphError("packed duplicate id")
			}
			seen[entry.ChunkID] = true
			ref := live[int64(entry.ChunkID)]
			retiredEntry := false
			if ref == nil {
				ref = old[int64(entry.ChunkID)]
				retiredEntry = true
			}
			if ref == nil || uint64(ref.n("offset_in_block")) != entry.Offset || uint64(ref.n("size_in_block")) != entry.Size {
				return obj, graphError("packed entry mapping")
			}
			if !retiredEntry && !matchesHexHash(block.GetChunk(k), chunks[int64(entry.ChunkID)].s("chunk_hash")) {
				return obj, graphError("packed chunk digest")
			}
		}
	}
	return obj, nil
}
func matchesHash(data, want []byte) bool { s := sha256.Sum256(data); return equalDigest(s[:], want) }
func matchesHexHash(data []byte, want string) bool {
	b, e := hex.DecodeString(want)
	return e == nil && matchesHash(data, b)
}
