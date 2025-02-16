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
    FOREIGN KEY (document_id) REFERENCES documents (id),
    PRIMARY KEY (document_id, sentence_index)
);

CREATE TABLE named_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    named_entity TEXT UNIQUE,
    fq INT
);

CREATE TABLE entity_occurrences_summary (
    id INTEGER PRIMARY KEY NOT NULL,
    normalized_entity_text TEXT,
    entity_id INTEGER NOT NULL,
    uniq_documents INTEGER,
    fq INTEGER,
    FOREIGN KEY (entity_id) REFERENCES named_entities (id)
    UNIQUE (normalized_entity_text, entity_id)
);

CREATE VIEW view_entity_occurrences_summary AS
SELECT eos.id,
    eos.normalized_entity_text,
    ne.named_entity,
    eos.uniq_documents,
    eos.fq
FROM entity_occurrences_summary eos 
JOIN named_entities ne ON eos.entity_id = ne.id;

CREATE TABLE IF NOT EXISTS entity_error_codes (
    error_id VARCHAR(20) PRIMARY KEY,
    error_description VARCHAR(255)
);

CREATE TABLE entity_occurrences (
    id INTEGER PRIMARY KEY NOT NULL,
    entity_text TEXT,
    span_start INTEGER,
    span_end INTEGER,
    error_id VARCHAR(20) DEFAULT NULL,
    entity_id INTEGER NOT NULL,
    document_id INTEGER NOT NULL,
    sentence_index INTEGER NOT NULL,
    summary_id INTEGER,
    intra_doc_fq INTEGER,
    tf REAL,
    inter_doc_fq INTEGER,
    tf_idf REAL,
    idf REAL,
    FOREIGN KEY (document_id) REFERENCES documents (id),
    FOREIGN KEY (document_id, sentence_index) REFERENCES sentences (document_id, sentence_index),
    FOREIGN KEY (entity_id) REFERENCES named_entities (id),
    FOREIGN KEY (summary_id) REFERENCES entity_occurrences_summary (id),
    FOREIGN KEY (error_id) REFERENCES entity_error_codes (error_id)
);

CREATE VIEW view_entity_occurrences AS
SELECT eo.id,
    eo.entity_text,
    eo.span_start,
    eo.span_end,
    ne.named_entity,
    error_code.error_id,
    doc.title,
    eo.sentence_index,
    eos.normalized_entity_text,
    eos.normalized_entity_text,
    eos.uniq_documents, 
    eo.tf,
    eo.tf_idf,
    eo.idf
FROM entity_occurrences eo
JOIN named_entities ne ON eo.entity_id = ne.id
JOIN documents doc ON eo.document_id = doc.id
LEFT JOIN entity_occurrences_summary eos ON eo.summary_id = eos.id
LEFT JOIN entity_error_codes error_code ON eo.error_id = error_code.error_id;


CREATE TABLE entity_cooccurrences_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    e1_id_normalized INTEGER NOT NULL,
    e2_id_normalized INTEGER NOT NULL,
    fq_document_level INTEGER,
    fq_document_level_normalized REAL,
    fq_sentence_level INTEGER,
    fq_sentence_level_normalized REAL,
    pmi REAL,
    FOREIGN KEY (e1_id_normalized) REFERENCES entity_occurrences_summary (id),
    FOREIGN KEY (e2_id_normalized) REFERENCES entity_occurrences_summary (id)
);

CREATE VIEW view_entity_cooccurrences_summary AS
SELECT ecs.id,
    e1.normalized_entity_text AS entity_text1,
    e2.normalized_entity_text AS entity_text2,
    ne1.named_entity AS named_entity1,
    ne2.named_entity AS named_entity2,
    ecs.fq_document_level,
    ecs.fq_document_level_normalized,
    ecs.fq_sentence_level,
    ecs.fq_sentence_level_normalized,
    ecs.pmi
FROM entity_cooccurrences_summary ecs
JOIN entity_occurrences_summary e1 ON ecs.e1_id_normalized = e1.id
JOIN entity_occurrences_summary e2 ON ecs.e2_id_normalized = e2.id
JOIN named_entities ne1 ON e1.entity_id = ne1.id
JOIN named_entities ne2 ON e2.entity_id = ne2.id;


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
    FOREIGN KEY (coocurences_summary_id) REFERENCES entity_cooccurrences_summary (id)
);

CREATE VIEW view_entity_cooccurrences AS
SELECT ec.id,
    ne1.named_entity AS entity1,
    ne2.named_entity AS entity2,
    eo1.entity_text AS entity_text1,
    eo2.entity_text AS entity_text2,
    error_code1.error_id AS error1,
    error_code2.error_id AS error2,
    eo1.sentence_index AS sent_index1,
    eo2.sentence_index AS sent_index2,
    eo1.document_id AS doc_id1,
    eo2.document_id AS doc_id2,
    doc1.title AS doc_title1,
    doc2.title AS doc_title2,
    eo1.span_start AS span_start1,
    eo1.span_end AS span_end1,
    eo2.span_start AS span_start2,
    eo2.span_end AS span_end2,
    ec.overlap,
    ec.weight,
    ec.sentence_distance
FROM entity_cooccurrences ec
JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
JOIN named_entities ne1 ON eo1.entity_id = ne1.id
JOIN named_entities ne2 ON eo2.entity_id = ne2.id
JOIN documents doc1 ON eo1.document_id = doc1.id
JOIN documents doc2 ON eo2.document_id = doc2.id
LEFT JOIN entity_error_codes error_code1 ON eo1.error_id = error_code1.error_id
LEFT JOIN entity_error_codes error_code2 ON eo2.error_id = error_code2.error_id;


CREATE TABLE source_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    file_name TEXT UNIQUE,
    file_size INTEGER,
    import_date DATETIME
);


