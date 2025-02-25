VIEW_PREFIX = "v_"

TABLE_NE = "eo"
TABLE_NE_CLASS = "named_entities"
TABLE_NE_LOOKUP = TABLE_NE + "_lookup"
TABLE_NE_AGGR = TABLE_NE + "_" + "aggr"
DIS_stem = "DIS"
PNM_stem = "PNM"
TABLE_NE_DIS = TABLE_NE + "_" + DIS_stem
TABLE_NE_PNM = TABLE_NE + "_" + PNM_stem

TABLE_COOCCURRENCES = "co"
TABLE_CO_AGGR = TABLE_COOCCURRENCES + "_aggregated"

TABLE_DOCS = "documents"
TABLE_SENTENCES = "sentences"
TABLE_ERROR = "eo_error_codes"



COL_NE_CLASS_ID = "entity_id"  # Named entity text column name
COL_NE_CLASS_NAME = "named_entity"  # Named entity text column name
COL_NE_TXT = "entity_text"  # Named entity text column name
COL_NE_TXT_NORM = "txt_norm"  # Normalized named entity text column name
COL_NE_DOC_ID = "document_id"  # Document ID column name
COL_NE_SENT_IDX = "sentence_index"  # Sentence index column name
COL_NE_SPAN_START = "span_start"  # Span start column name
COL_NE_SPAN_END = "span_end"  # Span end column name
COL_NE_ERROR_ID = "error_id"  # Error ID column name
COL_NE_OVERLAP = "overlap"  # Overlap column name
COL_NE_NORM_ID = "norm_id"  # Normalized ID column name
COL_NE_AGGREGATED_ID = "norm_id"  # Aggregated ID column name
COL_NE_FQ = "fq"  # Frequency column name
COL_NE_DOC_COUNT = "doc_count"  # Document count column name
COL_CO_SENT_DIST = "sent_distance"  # Sentence distance column name
COL_CO_AGGR_ID = "aggr_id"  # Aggregated ID column name
VIEW_NE = "view_ne"
VIEW_NE_RAW = "view_ne_raw"

VIEW_NE_COMP = VIEW_PREFIX + TABLE_NE + "_compiled"
VIEW_NE_STATS = VIEW_PREFIX + TABLE_NE + "_stats"
VIEW_COOCCURRENCES = VIEW_PREFIX + TABLE_COOCCURRENCES
VIEW_COOCCURRENCES_AGGREGATED = VIEW_PREFIX + TABLE_CO_AGGR
VIEW_DIS_PNM = VIEW_PREFIX + "_dis_pnm"

TABLE_DIS_PNM = "dis_pnm"

E1_ID = "e1_id"
E2_ID = "e2_id"
E1_NORM_ID = "e1_norm_id"
E2_NORM_ID = "e2_norm_id"
FQ_DOCUMENT_LEVEL = "fq_document_level"
FQ_SENTENCE_LEVEL = "fq_sentence_level"
UNIQ_DOCS = "uniq_docs"
PMI = "pmi"
DOC_ID = "doc_id"
NE_PRIMARY_ID = "id"
NEA_PRIMARY_ID = "norm_id"
BACKLINK_FOR_CO_OCCURRENCES = "backlink_for_co_occurrences"
