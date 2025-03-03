import logging
import sqlite3

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

CO_AGGR_ID = "CO_AGGR_ID"  # Aggregated ID column name
NE1 = "NE1"
NE2 = "NE2"
E1_ID = "E1_ID"
E2_ID = "E2_ID"
E1_NORM_ID = "E1_NORM_ID"
E2_NORM_ID = "E2_NORM_ID"
E1_CLASS_ID = "E1_CLASS_ID"
E2_CLASS_ID = "E2_CLASS_ID"

AVG_SENT_DIST = "avg_sentence_distance"
MIN_SENT_DIST = "min_sentence_distance"
MAX_SENT_DIST = "max_sentence_distance"


FQ_DOCUMENT_LEVEL = "FQ_DOCUMENT_LEVEL"
FQ_SENTENCE_LEVEL = "FQ_SENTENCE_LEVEL"
UNIQ_DOCS = "UNIQ_DOCS"
PMI = "PMI"
NPMI = "NPMI"
BACKLINK_FOR_CO_OCCURRENCES = "BACKLINK_FOR_CO_OCCURRENCES"  # Extra column, since the primary key is a composite key

VIEW_PREFIX = "v_"
VIEW_NE = "VIEW_NE"
VIEW_NE_RAW = "VIEW_NE_RAW"


VIEW_NE_STATS = VIEW_PREFIX + TABLE_NE + "_STATS"
VIEW_COOCCURRENCES = VIEW_PREFIX + TABLE_COOCCURRENCES
VIEW_COOCCURRENCES_AGGREGATED = VIEW_PREFIX + TABLE_CO_AGGR
VIEW_DIS_PNM = VIEW_PREFIX + "_DIS_PNM"

TABLE_DIS_PNM = "DIS_PNM"

TITLE = "TITLE"
SENT_COUNT = "SENT_COUNT"
WORD_COUNT = "WORD_COUNT"
TOKEN_COUNT = "TOKEN_COUNT"
ALPHA_COUNT = "ALPHA_COUNT"

# ----------------------
# Tables
# ----------------------
schema_create_table_docs = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_DOCS} (
                    {DOC_ID} INTEGER PRIMARY KEY,
                    {TITLE} TEXT,
                    {WORD_COUNT} INTEGER,
                    {TOKEN_COUNT} INTEGER,
                    {ALPHA_COUNT} INTEGER,
                    {SENT_COUNT} INTEGER
                );
                """

schema_create_table_sentences = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_SENTENCES} (
                    {DOC_ID} INTEGER,
                    {SENT_IDX} INTEGER,
                    {TXT} TEXT,
                    {WORD_COUNT} INTEGER,
{TOKEN_COUNT} INTEGER,
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
                    {TXT_NORM} TEXT, --  TXT_NORM in NE is a denormalization for performance - not a foreign key, NE_AGGR.TXT_NORM is the canonical version - the master record
                    {NE_NORM_ID} INTEGER,
                    {ERROR_ID} VARCHAR(20),
                    {NE_OVERLAP} BOOLEAN,
                    {SPAN_START} INTEGER,
                    {SPAN_END} INTEGER,
                    FOREIGN KEY ({DOC_ID}) REFERENCES {TABLE_DOCS} ({DOC_ID}),
                    FOREIGN KEY ({DOC_ID},{SENT_IDX}) REFERENCES {TABLE_SENTENCES} ({DOC_ID},{SENT_IDX}),
                    FOREIGN KEY ({CLASS_ID}) REFERENCES {TABLE_NE_CLASS} ({CLASS_ID}),
                    FOREIGN KEY ({NE_NORM_ID}) REFERENCES {TABLE_NE_AGGR} ({NE_NORM_ID}),
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
                    {NE_NORM_ID} INTEGER PRIMARY KEY AUTOINCREMENT,
                    {CLASS_ID} INTEGER,
                    {TXT_NORM} TEXT,
                    {FQ} INTEGER,
                    {UNIQ_DOCS} INTEGER,
                    UNIQUE ({CLASS_ID}, {TXT_NORM}),
                    FOREIGN KEY ({CLASS_ID}) REFERENCES {TABLE_NE_CLASS} ({CLASS_ID})
                );
                """

ne_class_schema = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_CLASS} (
                    {CLASS_ID} INTEGER PRIMARY KEY,
                    {NE_CLASS} TEXT,
                    {FQ} INTEGER
                );
                """

ne_error_schema = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_NE_ERROR} (
                    {ERROR_ID} VARCHAR(20) PRIMARY KEY,
                    {ERROR_DESC} TEXT,
                    UNIQUE ({ERROR_DESC})
                );
                """

SCHEMA_TABLE_ENTITY_COOCURRENCES = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_COOCCURRENCES} (
                    {E1_ID} INTEGER NOT NULL,
                    {E2_ID} INTEGER NOT NULL,
                    {SENT_DIST} INTEGER,
                    {CO_AGGR_ID} INTEGER,
                    PRIMARY KEY ({E1_ID}, {E2_ID})
                    FOREIGN KEY ({E1_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({E2_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({CO_AGGR_ID}) REFERENCES {TABLE_CO_AGGR}({CO_AGGR_ID})
                )
            """

SCHEMA_TABLE_DIS_PNM = f"""--sql
                CREATE TABLE IF NOT EXISTS {TABLE_DIS_PNM} (
                    {E1_ID} INTEGER NOT NULL,
                    {E2_ID} INTEGER NOT NULL,
                    {SENT_DIST} INTEGER,
                    {CO_AGGR_ID} INTEGER,
                    PRIMARY KEY ({E1_ID}, {E2_ID})
                    FOREIGN KEY ({E1_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({E2_ID}) REFERENCES {TABLE_NE}({NE_PRIMARY_ID}),
                    FOREIGN KEY ({CO_AGGR_ID}) REFERENCES {TABLE_CO_AGGR}({CO_AGGR_ID})
                )
            """

SCHEMA_TABLE_COOCCURRENCES_AGGR = f"""--sql
    CREATE TABLE IF NOT EXISTS {TABLE_CO_AGGR} (
        {E1_NORM_ID} INTEGER NOT NULL,
        {E2_NORM_ID} INTEGER NOT NULL,
        {CO_AGGR_ID} INTEGER AUTOINCREMENT,    -- Used as a lookup key instead of joining on the composite key
        {FQ_DOCUMENT_LEVEL} INTEGER DEFAULT NULL,
        {FQ_SENTENCE_LEVEL} INTEGER DEFAULT NULL,
        {UNIQ_DOCS} INTEGER DEFAULT NULL,
        {PMI} REAL DEFAULT NULL,

        PRIMARY KEY ({E1_NORM_ID}, {E2_NORM_ID})
        FOREIGN KEY ({E1_NORM_ID}) REFERENCES {TABLE_NE_AGGR}({NE_NORM_ID}),
        FOREIGN KEY ({E2_NORM_ID}) REFERENCES {TABLE_NE_AGGR}({NE_NORM_ID})
    )
"""
# ----------------------
# Views
# ----------------------
class View:
    """
    Represents an SQL view.
    'stmt' should only include the SELECT statement.
    """

    def __init__(self, main_table: str, select_stmt: str, suffix: str = None, columns: list[str] = None):
        self.name = "v_" + main_table + ("_" + suffix if suffix else "")
        self.stmt = select_stmt.replace("--sql", "").strip()
        self.columns = columns

    def create_if_not_exists(self, cursor: sqlite3.Cursor, logger: logging.Logger = None) -> bool:
        """Execute the CREATE VIEW IF NOT EXISTS statement for this view."""
        # Check if the view already exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='view' AND name='{self.name}';")
        if cursor.fetchone():
            if logger:
                logger.debug(f"View {self.name} already exists.")
            else:
                print(f"View {self.name} already exists.")
        else:
            cursor.execute(f"CREATE VIEW IF NOT EXISTS {self.name} AS {self.stmt}")
if logger:
                logger.info(f"Created view {self.name}.")
            else:
                print(f"Created view {self.name}.")

        if not self.validate(cursor):
            raise Exception(f"View validation of {self.name} failed.")

    def validate(self, cursor: sqlite3.Cursor) -> bool:
        """Validate the view by executing the SELECT statement."""
        cursor.execute(f"SELECT * FROM {self.name} LIMIT 1;")
        return bool(cursor.fetchall())

    def __str__(self) -> str:
        return self.name

    def drop(self, cursor: sqlite3.Cursor):
        """Drop the view if it exists."""
        cursor.execute(f"DROP VIEW IF EXISTS {self.name};")

    def refresh(self, cursor: sqlite3.Cursor, logger: logging.Logger = None):
        """Refresh the view by dropping and recreating it."""
        try:
            self.drop(cursor)
            self.create_if_not_exists(cursor)
logger.info(f"Refreshed view {self.name}.") if logger else print(
                f"Refreshed view {self.name}."
            )
        except Exception as e:
if logger:
                logger.error(f"Failed to refresh view {self.name}: {e}")
            else:
            print(f"Failed to refresh view {self.name}: {e}")

    def __repr__(self) -> str:
        return self.name


VIEW_NE_PRESENTATION = View(
    main_table=TABLE_NE,
    suffix="PRESENTATION",
    select_stmt=f"""--sql
            SELECT
                    ne.{NE_PRIMARY_ID},
                    ne.{TXT},
                    ne.{TXT_NORM},
                    nec.{NE_CLASS},
                    ne.{ERROR_ID},
                    ne.{NE_OVERLAP},
                    doc.title as document_title,
                    ne.{DOC_ID}
                FROM
                    {TABLE_NE} ne
                JOIN {TABLE_NE_CLASS} nec ON ne.{CLASS_ID} = nec.{CLASS_ID}
                JOIN {TABLE_DOCS} doc ON ne.{DOC_ID} = doc.{DOC_ID}
            """,
)

VIEW_NE_COMP = View(
    main_table=TABLE_NE,
    suffix="COMPILED",
    select_stmt=f"""--sql
            SELECT
                    ne.{NE_PRIMARY_ID},
                    ne.{TXT},
                    ne.{TXT_NORM},
                    nec.{NE_CLASS},
                    ne.{ERROR_ID},
                    ne.{NE_OVERLAP},
                    ne.{DOC_ID},
                    doc.{TITLE},
                    ne.{SENT_IDX},
                    ne.{SPAN_START},
                    ne.{SPAN_END}
                FROM
                    {TABLE_NE} ne
                JOIN {TABLE_NE_CLASS} nec ON ne.{CLASS_ID} = nec.{CLASS_ID}
                JOIN {TABLE_DOCS} doc ON ne.{DOC_ID} = doc.{DOC_ID}
            """,
)

VIEW_NE_VALIDATION_NORMALIZATION = View(
    main_table=TABLE_NE,
    suffix="VALIDATION_NORMALIZATION",
    select_stmt=f"""--sql
            SELECT
                    ne.{NE_PRIMARY_ID},
                    ne.{TXT},
                    ne.{TXT_NORM},
                    nea.{TXT_NORM} as canonical_text,
                    nec.{NE_CLASS},
                    ne.{ERROR_ID},
                    ne.{NE_OVERLAP},
                    doc.title as document_title,
                    ne.{DOC_ID}
                FROM
                    {TABLE_NE} ne
                JOIN {TABLE_NE_CLASS} nec ON ne.{CLASS_ID} = nec.{CLASS_ID}
                JOIN documents doc ON ne.{DOC_ID} = doc.{DOC_ID}
                LEFT JOIN {TABLE_NE_AGGR} nea ON ne.{NE_NORM_ID} = nea.{NE_NORM_ID}
            """,
)

VIEW_DIS_PNM_PRESENTATION = View(
    main_table=TABLE_DIS_PNM,
    suffix="PRESENTATION",
    select_stmt=f"""--sql
            SELECT
                    dis.{TXT} as dis,
                    pnm.{TXT} as pnm,
                    dis_pnm.{SENT_DIST},
                    doc.{TITLE} as document_title,
                    doc.{DOC_ID}
                FROM
                    {TABLE_DIS_PNM} dis_pnm
                JOIN {TABLE_NE} dis ON dis_pnm.{E1_ID} = dis.{NE_PRIMARY_ID}
                JOIN {TABLE_NE} pnm ON dis_pnm.{E2_ID} = pnm.{NE_PRIMARY_ID}
                JOIN documents doc ON dis.{DOC_ID} = doc.{DOC_ID} -- Join on document ID, only needs one of the entities
            """,
)

# ----------------------
# Indexes (ind + TABLE +)
# ----------------------
class Index:
    """
    Represents an SQL index.
    'stmt' should include 'CREATE INDEX IF NOT EXISTS' or 'CREATE UNIQUE INDEX IF NOT EXISTS'.
    """

    def __init__(
        self,
        table: str,
        columns: list[str],
        unique: bool = False,
        where: str = None,
        logger: logging.Logger = None,
    ):
        self.table = table
        self.columns = columns
        self.unique = unique
        self.where = where
        self.logger = logger
        self.stmt = self._create_stmt()

    def _create_stmt(self):
        """Create the SQL statement for this index."""
        unique = "UNIQUE " if self.unique else ""
        # Create safe index name - remove special chars and lowercase
        index_name = f"idx_{self.table}_" + "_".join(
            col.lower() for col in self.columns
        )
        columns_str = ", ".join(self.columns)
        where = f" WHERE {self.where}" if self.where else ""

        return f"CREATE {unique}INDEX IF NOT EXISTS {index_name} ON {self.table} ({columns_str}){where}"

    def __str__(self) -> str:
        return self.stmt

    def create_if_not_exists(self, cursor, analyze: bool = False) -> bool:
        """
        Execute the CREATE INDEX IF NOT EXISTS statement for this index.
        If 'analyze' is True, run ANALYZE on the indexed table if the index is created.
        """
        # Check if the index already exists
        cursor.execute(f"PRAGMA index_list({self.table});")
        indexes = cursor.fetchall()
        for index in indexes:
            if index[1] == f"idx_{self.table}_" + "_".join(
                col.lower() for col in self.columns
            ):
                if self.logger:
                    self.logger.info(
                        f"Index for table {self.table} with columns {self.columns} already exists."
                    )
                else:
                    print(
                        f"Index for table {self.table} with columns {self.columns} already exists."
                    )
                return False
        try:
            cursor.execute(self.stmt)
            if self.logger:
                self.logger.info(
                    f"Created index for table {self.table} with columns {self.columns}."
                )
            else:
                print(
                    f"Created index for table {self.table} with columns {self.columns}."
                )
            if analyze:
                self.analyze(cursor)
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to create index for table {self.table}: {e}")
            else:
                print(f"Failed to create index for table {self.table}: {e}")
            return False

    def analyze(self, cursor):
        """Run ANALYZE on the indexed table"""
        if self.logger:
            self.logger.info(f"Running ANALYZE on table {self.table}.")
        else:
            print(f"Running ANALYZE on table {self.table}.")

        cursor.execute(f"ANALYZE {self.table};")

    def drop(self, cursor):
        """Drop the index."""
        cursor.execute(f"DROP INDEX IF EXISTS {self.stmt};")



IDX_NE_ERROR_ID_NOT_NULL = Index(TABLE_NE, [ERROR_ID], where=f"{ERROR_ID} IS NOT NULL")



VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY = View(
    main_table=TABLE_DIS_PNM_AGGR,
    suffix="ROW_FACTORY",
    select_stmt=f"""--sql
            SELECT
                    co.{E1_NORM_ID},
                    co.{E2_NORM_ID},
                    ne1.{TXT_NORM} as DIS, -- only variables with non generic names
                    ne2.{TXT_NORM} as PNM, -- only variables with non generic names
                    -- ne1.{NE_CLASS} as {NE1 + "_" + NE_CLASS}, remove untill migration to non specific use for aggregate co table without classes
                    -- ne2.{NE_CLASS} as {NE2 + "_" + NE_CLASS},
                    co.{FQ_DOCUMENT_LEVEL} as {FQ_DOCUMENT_LEVEL},
                    co.{FQ_SENTENCE_LEVEL} as {FQ_SENTENCE_LEVEL},
                    co.{UNIQ_DOCS} as {UNIQ_DOCS},
                    co.{PMI} as {PMI},
                    co.{NPMI} as {NPMI},
                    co.{AVG_SENT_DIST} as {AVG_SENT_DIST},
                    co.{MIN_SENT_DIST} as {MIN_SENT_DIST},
                    co.{MAX_SENT_DIST} as {MAX_SENT_DIST},
                    ne1.{FQ} as {NE1 + "_" + FQ},
                    ne2.{FQ} as {NE2 + "_" + FQ},
                    ne1.{UNIQ_DOCS}  as {NE1 + "_" + UNIQ_DOCS},
                    ne2.{UNIQ_DOCS} as {NE2 + "_" + UNIQ_DOCS}
                FROM
                    {TABLE_DIS_PNM_AGGR} co
                JOIN {TABLE_NE_AGGR} ne1 ON co.{E1_NORM_ID} = ne1.{NE_NORM_ID}
                JOIN {TABLE_NE_AGGR} ne2 ON co.{E2_NORM_ID} = ne2.{NE_NORM_ID}
            """,
)


