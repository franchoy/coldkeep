#!/usr/bin/env python3
"""
gen_phase8_datasetF2.py - Generate GC dedup stress dataset (Phase 8 dataset F2).

Creates 20 "base" + 20 "derived" file pairs.
Each pair shares an identical 614,400-byte prefix (exact shared content).
Each file has an additional 614,400-byte unique suffix.
Total file size: 1,228,800 bytes (1200 KiB).

With v2-fastcdc (avg=64KB, max=128KB):
- ~19 chunks per file
- First ~9 chunks are shared between base and derived
- Last ~10 chunks are unique per file

With 1 MiB block target:
- Base file splits into ~2 blocks (B1 ~16 chunks, B2 ~3 chunks)
- Shared chunks in B1; some unique chunks in B1, overflow in B2
- After deleting base: B1 partially live (retained dead bytes from unique chunks in B1)
- B2 fully dead (reclaimable)

With 2 MiB block target:
- Base file fits in 1 block (1.2 MiB < 2 MiB)
- All shared + unique chunks in one block
- After deleting base: block partially live (more retained dead bytes than 1m case)

Usage:
    python3 scripts/gen_phase8_datasetF2.py /tmp/phase8_datasets/datasetF2
"""

import argparse
import os
import struct
import sys

SHARED_SIZE = 614_400   # 600 KiB
UNIQUE_SIZE = 614_400   # 600 KiB
FILE_SIZE = SHARED_SIZE + UNIQUE_SIZE  # 1200 KiB
PAIR_COUNT = 20


def pseudo_random_block(seed: int, size: int) -> bytes:
    """
    Generate deterministic pseudo-random bytes using a simple LCG seeded by
    seed. Content varies byte-to-byte to ensure CDC finds natural boundaries.
    """
    out = bytearray(size)
    state = seed & 0xFFFFFFFFFFFFFFFF
    i = 0
    while i < size:
        # 8 bytes at a time via LCG
        state = (state * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        chunk_bytes = struct.pack('<Q', state)
        end = min(i + 8, size)
        out[i:end] = chunk_bytes[:end - i]
        i += 8
    return bytes(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('output_dir', help='Output directory (files/ subdirectory will be created)')
    args = ap.parse_args()

    files_dir = os.path.join(args.output_dir, 'files')
    os.makedirs(files_dir, exist_ok=True)

    # Shared prefix is the same for each pair (pair index = seed for shared content)
    for i in range(PAIR_COUNT):
        pair_seed = 0xBEEF_0000 + i
        shared_content = pseudo_random_block(pair_seed, SHARED_SIZE)

        # Base file: shared prefix + unique suffix (seed derived from pair+role)
        base_unique = pseudo_random_block(0xDEAD_0000 + i, UNIQUE_SIZE)
        base_path = os.path.join(files_dir, f'base_{i+1:04d}.bin')
        with open(base_path, 'wb') as f:
            f.write(shared_content)
            f.write(base_unique)

        # Derived file: same shared prefix + different unique suffix
        derived_unique = pseudo_random_block(0xCAFE_0000 + i, UNIQUE_SIZE)
        derived_path = os.path.join(files_dir, f'derived_{i+1:04d}.bin')
        with open(derived_path, 'wb') as f:
            f.write(shared_content)
            f.write(derived_unique)

    total = PAIR_COUNT * 2
    total_mb = total * FILE_SIZE / (1024 * 1024)
    print(f'Generated {total} files ({PAIR_COUNT} base + {PAIR_COUNT} derived) in {files_dir}')
    print(f'Each file: {FILE_SIZE // 1024} KiB  |  Total: {total_mb:.1f} MiB')
    print(f'Shared prefix per pair: {SHARED_SIZE // 1024} KiB')
    print(f'Unique suffix per file: {UNIQUE_SIZE // 1024} KiB')


if __name__ == '__main__':
    main()
