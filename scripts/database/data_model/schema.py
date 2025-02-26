
TABLE_DOCS = "documents"
TABLE_SENTENCES = "sentences"

TABLE_NE = "NE"
TABLE_NE_CLASS = TABLE_NE + "_CLASS"
TABLE_NE_LOOKUP = TABLE_NE + "_LOOKUP"

AGGREGATED_SUFFIX = "AGGR"
TABLE_NE_AGGR = TABLE_NE + "_" + AGGREGATED_SUFFIX
TABLE_NE_ERROR = TABLE_NE + "_ERROR_CODES"


DIS_stem = "DIS"
PNM_stem = "PNM"

TABLE_NE_DIS = TABLE_NE + "_" + DIS_stem
TABLE_NE_PNM = TABLE_NE + "_" + PNM_stem

TABLE_COOCCURRENCES = "CO"
TABLE_CO_AGGR = TABLE_COOCCURRENCES + "_" + AGGREGATED_SUFFIX



# ----------------------
# Columns
# ----------------------
TXT = "TXT"  # Named entity text column name
TXT_NORM = "TXT_NORM"  # Named entity text column name
CLASS_ID = "NE_CLASS_ID"  # Named entity text column name
NE_CLASS = "NE_CLASS"  # Named entity text column name
DOC_ID = "DOC_ID"  # Document ID column name
SENT_IDX = "SENT_IDX"  # Sentence index column name
SPAN_START = "SPAN_START"  # Span start column name
ERROR_DESC = "ERROR_DESC"  # Error description column name
SPAN_START = "SPAN_START"  # Span start column name
SPAN_END = "SPAN_END"  # Span end column name
ERROR_ID = "ERROR_ID"  # Error ID column name
NE_OVERLAP = "OVERLAP"  # Overlap column name
NE_PRIMARY_ID = "NE_ID"
NE_NORM_ID = "NE_NORM_ID"  # Normalized ID column name
FQ = "FQ"  # Frequency column name

NE_LOOKUP_ID = "NE_LOOKUP_ID"  # Named entity lookup ID column name
# Co-occurrence specific columns
DOC_COUNT = "DOC_COUNT"  # Document count column name
SENT_DIST = "SENT_DISTANCE"  # Sentence distance column name

AGGR_ID = "AGGR_ID"  # Aggregated ID column name
E1_ID = "E1_ID"
E2_ID = "E2_ID"
E1_NORM_ID = "E1_NORM_ID"
E2_NORM_ID = "E2_NORM_ID"
E1_CLASS_ID = "E1_CLASS_ID"
E2_CLASS_ID = "E2_CLASS_ID"

FQ_DOCUMENT_LEVEL = "FQ_DOCUMENT_LEVEL"
FQ_SENTENCE_LEVEL = "FQ_SENTENCE_LEVEL"
UNIQ_DOCS = "UNIQ_DOCS"
PMI = "PMI"
BACKLINK_FOR_CO_OCCURRENCES = "BACKLINK_FOR_CO_OCCURRENCES" # Extra column, since the primary key is a composite key

VIEW_PREFIX = "v_"
VIEW_NE = "VIEW_NE"
VIEW_NE_RAW = "VIEW_NE_RAW"

VIEW_NE_COMP = VIEW_PREFIX + TABLE_NE + "_COMPILED"
VIEW_NE_STATS = VIEW_PREFIX + TABLE_NE + "_STATS"
VIEW_COOCCURRENCES = VIEW_PREFIX + TABLE_COOCCURRENCES
VIEW_COOCCURRENCES_AGGREGATED = VIEW_PREFIX + TABLE_CO_AGGR
VIEW_DIS_PNM = VIEW_PREFIX + "_DIS_PNM"

TABLE_DIS_PNM = "DIS_PNM"

TITLE = "TITLE"
WORD_COUNT = "WORD_COUNT"
SENT_COUNT = "SENT_COUNT"
ALPHA_COUNT = "ALPHA_COUNT"

# ----------------------
# Tables
# ----------------------
schema_create_table_docs = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_DOCS} (
                    {DOC_ID} INTEGER PRIMARY KEY,
                    {TITLE} TEXT,
                    {WORD_COUNT} INTEGER,
                    {SENT_COUNT} INTEGER,
                    {ALPHA_COUNT} INTEGER
                );
                """

schema_create_table_sentences = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_SENTENCES} (
                    {DOC_ID} INTEGER,
                    {SENT_IDX} INTEGER,
                    {TXT} TEXT,
                    {WORD_COUNT} INTEGER,
                    {ALPHA_COUNT} INTEGER,
                    PRIMARY KEY ({DOC_ID}, {SENT_IDX}),
                    FOREIGN KEY ({DOC_ID}) REFERENCES {TABLE_DOCS} ({DOC_ID})
                );
                """

schema_create_table_ne = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE} (
                    {NE_PRIMARY_ID} INTEGER PRIMARY KEY,
                    {TXT} TEXT,
                    {CLASS_ID} INTEGER,
                    {DOC_ID} INTEGER,
                    {SENT_IDX} INTEGER,
                    {TXT_NORM} TEXT, -- Normalized text if we want to store it here for simplicity
                    {NE_NORM_ID} INTEGER,
                    {ERROR_ID} VARCHAR(20),
                    {NE_OVERLAP} BOOLEAN,
                    {SPAN_START} INTEGER,
                    {SPAN_END} INTEGER,
                    FOREIGN KEY ({DOC_ID}) REFERENCES {TABLE_DOCS} ({DOC_ID}),
                    FOREIGN KEY ({DOC_ID},{SENT_IDX}) REFERENCES {TABLE_SENTENCES} ({DOC_ID},{SENT_IDX}),
                    FOREIGN KEY ({CLASS_ID}) REFERENCES {TABLE_NE_CLASS} ({CLASS_ID}),
                    FOREIGN KEY ({TXT_NORM}) REFERENCES {TABLE_NE_AGGR} ({NE_NORM_ID}),
                    FOREIGN KEY ({ERROR_ID}) REFERENCES {TABLE_NE_ERROR} ({ERROR_ID})
                );
                """

schema_create_table_ne_lookup = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_LOOKUP} (
                    {NE_LOOKUP_ID} INTEGER PRIMARY KEY, -- 1:1 relation with {TABLE_NE}
                    {CLASS_ID} INTEGER,
                    {TXT_NORM} TEXT,
                    {NE_NORM_ID} INTEGER, -- Reference to {TABLE_NE_LOOKUP}
                    FOREIGN KEY ({NE_LOOKUP_ID}) REFERENCES {TABLE_NE} ({NE_PRIMARY_ID}), -- 1:1 relation with {TABLE_NE}
                    FOREIGN KEY ({NE_NORM_ID}) REFERENCES {TABLE_NE_AGGR} ({NE_NORM_ID}) -- Reference to {TABLE_NE_AGGR}, acts as a middleman
                    )"""

schema_create_table_ne_aggregated = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_AGGR} (
                    {NE_NORM_ID} INTEGER,
                    {CLASS_ID} INTEGER,
                    {TXT_NORM} TEXT,
                    PRIMARY KEY ({NE_NORM_ID}, {CLASS_ID}),
                    FOREIGN KEY ({CLASS_ID}) REFERENCES {TABLE_NE_CLASS} ({CLASS_ID})
                );
                """

schema_create_table_ne_class = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_CLASS} (
                    {CLASS_ID} INTEGER PRIMARY KEY,
                    {NE_CLASS} TEXT,
                    {FQ} INTEGER
                );
                """

schema_create_table_ne_error = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_ERROR} (
                    {ERROR_ID} VARCHAR(20) PRIMARY KEY,
                    {ERROR_DESC} TEXT,
                    UNIQUE ({ERROR_DESC})
                );
                """

