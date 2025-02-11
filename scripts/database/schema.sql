CREATE TABLE documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    title TEXT,
    word_count INTEGER,
    token_count INTEGER,
    alpha_count INTEGER
);

CREATE TABLE sentences (
    text TEXT,
    sentence_index INTEGER,
    document_id INTEGER NOT NULL,
    word_count INTEGER,
    token_count INTEGER,
    alpha_count INTEGER,
    FOREIGN KEY (document_id) REFERENCES documents (id)
    PRIMARY KEY (document_id, sentence_index)
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
    summary_id INTEGER,
    intra_doc_fq INTEGER,
    tf REAL,
    inter_doc_fq INTEGER,
    tf_idf REAL,
    idf REAL,
    FOREIGN KEY (sentence_id) REFERENCES sentences (id),
    FOREIGN KEY (entity_id) REFERENCES named_entities (id)
    FOREIGN KEY (summary_id) REFERENCES entity_occurrence_summary (id)
);

CREATE TABLE entity_occurrences_summary (
    id INTEGER PRIMARY KEY NOT NULL,
    normalized_entity_text TEXT,
    fq_sentence_level INTEGER,
    fq_document_level INTEGER,
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
    file_size INTEGER,
    import_date DATETIME
);
