-- Indexes for entity_occurrences table
CREATE INDEX IF NOT EXISTS idx_entity_occurrences_id ON entity_occurrences(id);
CREATE INDEX IF NOT EXISTS idx_entity_occurrences_entity_text ON entity_occurrences(entity_text);
CREATE INDEX IF NOT EXISTS idx_entity_occurrences_document ON entity_occurrences(document_id, sentence_index);

-- Indexes for entity_cooccurrences_summary table
CREATE INDEX IF NOT EXISTS idx_entity_cooc_summary_e1 ON entity_cooccurrences_summary(e1_id);
CREATE INDEX IF NOT EXISTS idx_entity_cooc_summary_e2 ON entity_cooccurrences_summary(e2_id);
CREATE INDEX IF NOT EXISTS idx_entity_cooc_summary_doc_freq ON entity_cooccurrences_summary(fq_document_level DESC);
CREATE INDEX IF NOT EXISTS idx_entity_cooc_summary_combined ON entity_cooccurrences_summary(e1_id, e2_id, fq_document_level DESC);

-- Index for entity_occurrences_summary normalized text
CREATE INDEX IF NOT EXISTS idx_entity_occurrences_summary_normalized_text ON entity_occurrences_summary(normalized_entity_text);

-- Indexes for entity_cooccurrences table
CREATE INDEX IF NOT EXISTS idx_entity_cooc_e1 ON entity_cooccurrences(e1_id);
CREATE INDEX IF NOT EXISTS idx_entity_cooc_e2 ON entity_cooccurrences(e2_id);
CREATE INDEX IF NOT EXISTS idx_entity_cooc_summary ON entity_cooccurrences(coocurences_summary_id);