CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    title TEXT,
    word_count INTEGER,
    token_count INTEGER,
    alpha_count INTEGER
);

CREATE TABLE sentences (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    text TEXT,
    sentence_index INTEGER,
    word_count INTEGER,
    token_count INTEGER,
    alpha_count INTEGER,
    document_id INTEGER NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents (id)
);

CREATE TABLE named_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    named_entity TEXT UNIQUE
);

CREATE TABLE entity_occurrences (
    id INTEGER PRIMARY KEY NOT NULL,
    entity_text TEXT,
    span_start INTEGER,
    span_end INTEGER,
    entity_id INTEGER NOT NULL,
    sentence_id INTEGER NOT NULL,
    intra_doc_fq INTEGER,
    tf REAL,
    inter_doc_fq INTEGER,
    tf_idf REAL,
    idf REAL,
    FOREIGN KEY (sentence_id) REFERENCES sentences (id),
    FOREIGN KEY (entity_id) REFERENCES named_entities (id)
);

CREATE TABLE entity_cooccurrences (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    e1_id INTEGER NOT NULL,
    e2_id INTEGER NOT NULL,
    overlap BOOLEAN,
    weight REAL,
    sentence_distance INTEGER,
    coocurences_summary_id INTEGER,
    FOREIGN KEY (e1_id) REFERENCES entity_occurrences (id),
    FOREIGN KEY (e2_id) REFERENCES entity_occurrences (id),
    FOREIGN KEY (coocurences_summary_id) REFERENCES cooccurrence_summary (id)
);

CREATE TABLE cooccurrence_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    fq_document_level INTEGER,
    fq_document_level_normalized REAL,
    fq_sentence_level INTEGER,
    fq_sentence_level_normalized REAL
);

CREATE TABLE coentity_summary (
    id INTEGER PRIMARY KEY NOT NULL
);

CREATE TABLE source_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    file_name TEXT UNIQUE,
    file_size INTEGER
);
