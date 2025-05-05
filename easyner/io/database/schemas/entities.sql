CREATE TABLE IF NOT EXISTS entities (
    entity_id BIGINT DEFAULT nextval('entity_id_seq'),
    article_id INTEGER,
    sentence_id INTEGER,
    entity VARCHAR,
    start_pos INTEGER,
    end_pos INTEGER,
    inference_model VARCHAR DEFAULT NULL,
    inference_model_metadata VARCHAR DEFAULT NULL,
    PRIMARY KEY(entity_id),
    FOREIGN KEY (article_id, sentence_id) REFERENCES sentences(article_id, sentence_id)
);
