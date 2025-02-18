CREATE TABLE
    documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        title TEXT,
        word_count INTEGER,
        token_count INTEGER,
        alpha_count INTEGER
    );

CREATE TABLE
    sentences (
        text TEXT,
        sentence_index INTEGER,
        document_id INTEGER NOT NULL,
        word_count INTEGER,
        token_count INTEGER,
        alpha_count INTEGER,
        FOREIGN KEY (document_id) REFERENCES documents (id),
        PRIMARY KEY (document_id, sentence_index)
    );

CREATE TABLE
    named_entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        named_entity TEXT UNIQUE,
        fq INT
    );

CREATE TABLE
    entity_occurrences_summary (
        id INTEGER PRIMARY KEY NOT NULL,
        normalized_entity_text TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        uniq_documents INTEGER,
        fq INTEGER,
        UNIQUE(normalized_entity_text, entity_id),
        FOREIGN KEY (entity_id) REFERENCES named_entities (id)
    );

CREATE VIEW
    view_entity_occurrences_summary AS
SELECT
    eos.id,
    eos.normalized_entity_text,
    ne.named_entity,
    eos.uniq_documents,
    eos.fq
FROM
    entity_occurrences_summary eos
    JOIN named_entities ne ON eos.entity_id = ne.id;

CREATE TABLE
    IF NOT EXISTS entity_error_codes (
        error_id VARCHAR(20) PRIMARY KEY,
        error_description VARCHAR(255)
    );

CREATE TABLE
    entity_occurrences (
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

CREATE INDEX idx_entity_occurrences_error_check 
ON entity_occurrences(summary_id, error_id)
WHERE error_id IS NOT NULL;

CREATE VIEW
    view_entity_occurrences AS
SELECT
    eo.id,
    eo.entity_text,
    eo.span_start,
    eo.span_end,
    ne.named_entity,
    error_code.error_id,
    doc.title,
    eo.sentence_index,
    eos.normalized_entity_text,
    eos.uniq_documents,
    eo.tf,
    eo.tf_idf,
    eo.idf
FROM
    entity_occurrences eo
    JOIN named_entities ne ON eo.entity_id = ne.id
    JOIN documents doc ON eo.document_id = doc.id
    LEFT JOIN entity_occurrences_summary eos ON eo.summary_id = eos.id
    LEFT JOIN entity_error_codes error_code ON eo.error_id = error_code.error_id;

CREATE VIEW valid_entity_occurrences AS
SELECT
    eo.id,
    eo.entity_text,
    eo.span_start,
    ne.named_entity,
    eo.entity_id,
    error_code.error_id,
    doc.title,
    eo.sentence_index,
    eos.normalized_entity_text,
    eos.uniq_documents,
    eo.tf,
    eo.inter_doc_fq,
    eo.tf_idf,
    eo.idf
FROM
    entity_occurrences eo
    JOIN named_entities ne ON eo.entity_id = ne.id
    JOIN documents doc ON eo.document_id = doc.id
    LEFT JOIN entity_occurrences_summary eos ON eo.summary_id = eos.id
    LEFT JOIN entity_error_codes error_code ON eo.error_id = error_code.error_id
WHERE
    eo.error_id IS NULL;
CREATE TABLE
    entity_cooccurrences_summary (
        e1_id_normalized INTEGER NOT NULL,
        e2_id_normalized INTEGER NOT NULL,
        fq_document_level INTEGER DEFAULT NULL,
        fq_sentence_level INTEGER DEFAULT NULL,
        uniq_documents INTEGER DEFAULT NULL,
        pmi REAL DEFAULT NULL,
        PRIMARY KEY (e1_id_normalized, e2_id_normalized),
        CHECK (e1_id_normalized <= e2_id_normalized),
        UNIQUE (e1_id_normalized, e2_id_normalized), -- Critical for batch safety
        FOREIGN KEY (e1_id_normalized) REFERENCES entity_occurrences_summary (id),
        FOREIGN KEY (e2_id_normalized) REFERENCES entity_occurrences_summary (id)
    );

CREATE VIEW
    view_entity_cooccurrences_summary AS
SELECT
    eos1.normalized_entity_text AS entity_text1,
    eos2.normalized_entity_text AS entity_text2,
    ne1.named_entity AS named_entity1,
    ne2.named_entity AS named_entity2,
    ecs.fq_document_level,
    ecs.fq_sentence_level,
    ecs.pmi,
    eo1.error_id AS error_id1,
    eo2.error_id AS error_id2
FROM
    entity_cooccurrences_summary ecs
    JOIN entity_occurrences_summary eos1 ON ecs.e1_id_normalized = eos1.id
    JOIN entity_occurrences_summary eos2 ON ecs.e2_id_normalized = eos2.id
    JOIN entity_occurrences eo1 ON eos1.id = eo1.id
    JOIN entity_occurrences eo2 ON eos2.id = eo2.id
    JOIN named_entities ne1 ON eos1.entity_id = ne1.id
    JOIN named_entities ne2 ON eos2.entity_id = ne2.id;

CREATE VIEW
    view_disease_phenomena_summary AS
SELECT
    eos1.normalized_entity_text AS disease,
    eos2.normalized_entity_text AS phenomenon,
    ecs.fq_document_level,
    ecs.fq_sentence_level,
    ecs.pmi,
    eos1.fq AS fq_disease,
    eos2.fq AS fq_phenomenon,
    eos1.uniq_documents AS uniq_documents_disease,
    eos2.uniq_documents AS uniq_documents_phenomenon
FROM
    entity_cooccurrences_summary ecs
    JOIN entity_occurrences_summary eos1 ON ecs.e1_id_normalized = eos1.id
    JOIN entity_occurrences_summary eos2 ON ecs.e2_id_normalized = eos2.id
    

WHERE
    eos1.entity_id = (
        SELECT
            id
        FROM
            named_entities
        WHERE
            named_entity = 'DIS'
    )
    AND eos2.entity_id = (
        SELECT
            id
        FROM
            named_entities
        WHERE
            named_entity = 'PNM'
    )
ORDER BY
    ecs.pmi DESC;

CREATE TABLE
    entity_cooccurrences (
        e1_id INTEGER NOT NULL,
        e2_id INTEGER NOT NULL,
        overlap BOOLEAN,
        weight REAL,
        sentence_distance INTEGER,
        summary_id INTEGER,
        PRIMARY KEY (e1_id, e2_id),
        CHECK (e1_id <= e2_id), -- enforce ordering so that (e1, e2) is always ordered with e1 < e2, if not self referential
        UNIQUE (e1_id, e2_id),
        FOREIGN KEY (e1_id) REFERENCES entity_occurrences (id),
        FOREIGN KEY (e2_id) REFERENCES entity_occurrences (id)
    ) WITHOUT ROWID;

-- disable rowid to enforce the composite primary key, ~30 % storgare savings for this table
CREATE VIEW
    view_entity_cooccurrences AS
SELECT
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
FROM
    entity_cooccurrences ec
    JOIN entity_occurrences eo1 ON ec.e1_id = eo1.id
    JOIN entity_occurrences eo2 ON ec.e2_id = eo2.id
    JOIN named_entities ne1 ON eo1.entity_id = ne1.id
    JOIN named_entities ne2 ON eo2.entity_id = ne2.id
    JOIN documents doc1 ON eo1.document_id = doc1.id
    JOIN documents doc2 ON eo2.document_id = doc2.id
    LEFT JOIN entity_error_codes error_code1 ON eo1.error_id = error_code1.error_id
    LEFT JOIN entity_error_codes error_code2 ON eo2.error_id = error_code2.error_id;

CREATE TABLE
    source_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        file_name TEXT UNIQUE,
        file_size INTEGER,
        import_date DATETIME
    );

CREATE INDEX idx_entity_occurrences_error 
ON entity_occurrences(entity_text, error_id)
WHERE error_id IS NOT NULL;

CREATE VIEW view_entity_summary_violations AS
SELECT DISTINCT
    eos.id as summary_id,
    eos.normalized_entity_text,
    ne.named_entity,
    eo.error_id,
    eo.entity_text
FROM entity_occurrences_summary eos
JOIN entity_occurrences eo ON eo.summary_id = eos.id
JOIN named_entities ne ON ne.id = eos.entity_id
WHERE eo.error_id IS NOT NULL;