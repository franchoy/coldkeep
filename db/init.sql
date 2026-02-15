DROP TABLE IF EXISTS file_chunk;
DROP TABLE IF EXISTS logical_file;
DROP TABLE IF EXISTS chunk;
DROP TABLE IF EXISTS container;

CREATE TABLE logical_file (
    id SERIAL PRIMARY KEY,
    original_name TEXT NOT NULL,
    total_size BIGINT NOT NULL,
    file_hash CHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE container (
    id SERIAL PRIMARY KEY,
    filename TEXT NOT NULL,
    current_size BIGINT NOT NULL,
    max_size BIGINT NOT NULL,
    sealed BOOLEAN DEFAULT FALSE,
    compression_algorithm TEXT DEFAULT 'none',
    compressed_size BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE chunk (
    id SERIAL PRIMARY KEY,
    sha256 CHAR(64) NOT NULL UNIQUE,
    size INTEGER NOT NULL,
    container_id INTEGER REFERENCES container(id),
    chunk_offset BIGINT NOT NULL,
    ref_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);


CREATE TABLE file_chunk (
    file_id INTEGER REFERENCES logical_file(id) ON DELETE CASCADE,
    chunk_id INTEGER REFERENCES chunk(id),
    chunk_order INTEGER NOT NULL,
    PRIMARY KEY (file_id, chunk_order)
);
