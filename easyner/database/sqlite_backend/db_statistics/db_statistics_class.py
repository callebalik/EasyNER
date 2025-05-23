import logging
import os
import sqlite3
from typing import Optional

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
from pandas import DataFrame

# We use this decorator as an instance method of CacheManager
# So we need to create a cache_manager instance to use its cached decorator
from easyner.database.sqlite_backend.core.cache_singleton import cached
from easyner.database.sqlite_backend.data_model.schema import (
    CLASS_ID,
    DOC_ID,
    ERROR_ID,
    NE_CLASS,
    NE_OVERLAP,
    SENT_IDX,
    TABLE_DIS_PNM,
    TABLE_DOCS,
    TABLE_NE,
    TABLE_NE_CLASS,
    TABLE_SENTENCES,
    VIEW_NE_CLEAN,
    VIEW_NE_COMP,
)
from easyner.database.sqlite_backend.db_data_exchanger import DBDataExchanger


class DBStatistics:

    def __init__(
        self,
        conn: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        logger: logging.Logger,
        data_exchanger: DBDataExchanger,
    ):
        self.conn = conn
        self.cursor = conn.cursor()
        self.logger = logger
        self._results_dir = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "results",
        )
        self.data_exchanger = data_exchanger

    def _export_df_(self, results: DataFrame, filename, overwrite: bool = True):
        """Export results to a CSV file.

        :param results: The results to export.
        :param filename: The file to export the results to.
        """
        filename = os.path.join(self._results_dir, filename)
        if os.path.exists(filename) and not overwrite:
            self.logger.error(
                f"File {filename} already exists. Set overwrite=True to overwrite.",
            )
            return

        results.to_csv(filename, index=False)
        self.logger.info(f"Results exported to {filename}")

    @property
    def size(self):
        """Get the size of the database file in bytes.

        :return: The size of the database file in bytes.
        """
        self.cursor.execute(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();",
        )
        return self.cursor.fetchone()[0]

    @property
    def total_source_size(self):
        """Get the total size of the source files in bytes.
        Source files sizes are stored in MB in the database.

        :return: The total size of the source files in bytes.
        """
        self.cursor.execute("SELECT SUM(file_size) FROM source_files;")
        mb_size = self.cursor.fetchone()[0] or 0
        return mb_size * 1024 * 1024  # Convert MB to bytes

    @property
    def compression_ratio(self):
        """Get the compression ratio of the database file compared to source files.

        :return: The compression ratio of the database file.
        """
        source_size = self.total_source_size
        if source_size == 0:
            return 0.0
        return self.size / source_size

    def _format_size(self, size_bytes):
        """Convert size in bytes to human readable format.

        :param size_bytes: Size in bytes
        :return: String with formatted size
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024 or unit == "TB":
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024

    @property
    @cached(prefix="stats.source_file_count")
    def info_compression(self) -> None:
        """Summary of the compression ratio of the database file compared to source files."""
        db_size = self._format_size(self.size)
        source_size = self._format_size(self.total_source_size)  # Already in bytes
        print(f"Database size: {db_size}")
        print(f"Total source file size: {source_size}")
        print(f"Compression ratio: {self.compression_ratio:.2f}")

    @property
    @cached(ttl_seconds=3600, prefix="stats.document_count")
    def document_count(self):
        """Get the total number of documents in the database.
        Uses cache if available to avoid expensive database query.

        :return: The number of documents.
        """
        self.cursor.execute("SELECT COUNT(*) FROM documents;")
        return self.cursor.fetchone()[0]

    @property
    @cached(ttl_seconds=3600, prefix="stats.sentence_count")
    def sentence_count(self):
        """Get the total number of sentences in the database.
        Uses cache if available to avoid expensive database query.

        :return: The number of sentences.
        """
        self.cursor.execute("SELECT COUNT(*) FROM sentences;")
        return self.cursor.fetchone()[0]

    @property
    @cached(ttl_seconds=3600, prefix="stats.named_entity_classes")
    def named_entity_classes_count(self):
        """Get the total number of named entities in the database.
        Uses cache if available to avoid expensive database query.

        :return: The number of named entity classes.
        """
        self.cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NE_CLASS};")
        return self.cursor.fetchone()[0]

    @property
    @cached()
    def total_valid_named_entities(self):
        """Get the total number of valid named entities in the database.
        Uses
        :return: The number of valid named entities.
        """
        self.cursor.execute(f"SELECT COUNT(*) FROM {VIEW_NE_CLEAN}")
        return self.cursor.fetchone()[0]

    def debug_database_schema(self) -> None:
        """Examine actual database schema and verify table/column names."""
        print("\n=== DATABASE SCHEMA DIAGNOSTIC ===")

        # Get list of all tables
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in self.cursor.fetchall()]
        print(f"Tables in database: {tables}")

        # Check if our expected tables exist
        expected_tables = [TABLE_NE, TABLE_DOCS, TABLE_NE_CLASS]
        for table in expected_tables:
            exists = table in tables
            print(f"Table '{table}' {'EXISTS' if exists else 'MISSING'}")

            if exists:
                # Get column names for this table
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in self.cursor.fetchall()]
                print(f"  Columns: {columns}")

                # Check row count
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = self.cursor.fetchone()[0]
                print(f"  Row count: {count}")

                # Sample data if available
                if count > 0:
                    self.cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                    sample = self.cursor.fetchone()
                    print(f"  Sample: {sample}")

    @cached(ttl_seconds=2150, prefix="stats.named_entities_count")
    def named_entities_count(
        self,
        ne_class: str = None,
        include_errors: bool = False,
        include_ambiguous: bool = False,
        include_overlaps: bool = False,
    ):
        """Get the total number of entity occurrences in the database."""
        try:
            # Basic query without conditions first
            query = f"SELECT COUNT(*) FROM {TABLE_NE}"

            conditions = []
            params = []

            # Add conditions only if needed
            if ne_class:
                class_id = self.data_exchanger.get_named_entity_class_id(ne_class)
                if class_id:
                    conditions.append(f"{CLASS_ID} = ?")
                    params.append(class_id)

            if not include_errors:
                conditions.append(f"{ERROR_ID} IS NULL")

            if not include_overlaps:
                conditions.append(f"{NE_OVERLAP} IS 0")

            # Add WHERE clause only if we have conditions
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            self.logger.debug(f"Executing named_entities_count query: {query}")
            self.logger.debug(f"With parameters: {params}")

            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            count = self.cursor.fetchone()[0]
            return count

        except Exception as e:
            self.logger.error(f"Error in named_entities_count: {e}")
            return 0

    @property
    @cached(ttl_seconds=3600, prefix="stats.entity_cooccurrence_count")
    def get_entity_cooccurrence_count(self):
        """Get the total number of entity cooccurrences in the database.
        Uses cache if available to avoid expensive database query.

        :return: The number of entity cooccurrences.
        """
        query = f"""
        SELECT COUNT(*) FROM {TABLE_DIS_PNM}
        """
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    # -------- Data for data flow analysis --------
    def documents_with_entities(
        self,
        included_ne_classes: list[str] = None,
        excluded_ne_classes: list[str] = None,
        include_errors: bool = False,
        include_ambiguous: bool = False,
        include_overlaps: bool = False,
    ):
        """Get counts of documents with named entities meeting specified criteria.

        Args:
            included_ne_classes: List of entity classes that MUST be present in the document
            excluded_ne_classes: List of entity classes that MUST NOT be present in the document
            include_errors: Whether to include entities with errors
            include_ambiguous: Whether to include ambiguous entities
            include_overlaps: Whether to include overlapping entities

        Returns:
            int: Count of documents meeting the criteria

        """
        # Convert None to empty lists for consistent handling
        included_ne_classes = included_ne_classes or []
        excluded_ne_classes = excluded_ne_classes or []

        included_ne_ids = [
            self.data_exchanger.get_named_entity_class_id(ne_class)
            for ne_class in included_ne_classes
        ]
        excluded_ne_ids = [
            self.data_exchanger.get_named_entity_class_id(ne_class)
            for ne_class in excluded_ne_classes
        ]

        # Basic case: all documents with any entity
        if not included_ne_classes and not excluded_ne_classes:
            query = f"""--sql
            SELECT COUNT(DISTINCT ne.{DOC_ID})
            FROM {TABLE_NE} ne
            WHERE 1=1
            """

            params = []

            if not include_errors:
                query += f" AND {ERROR_ID} IS NULL"
            if not include_overlaps:
                query += f" AND {NE_OVERLAP} IS 0"

        # Complex case with specific entity class requirements
        else:
            query = f"""--sql
            SELECT COUNT(DISTINCT d.{DOC_ID})
            FROM {TABLE_DOCS} d
            """
            params = []

            # For each included class, add an EXISTS subquery
            for i, ne_id in enumerate(included_ne_ids):
                query += f"""
                {'WHERE' if i == 0 and not params else 'AND'} EXISTS (
                    SELECT 1 FROM {TABLE_NE} ne{i}
                    WHERE ne{i}.{DOC_ID} = d.{DOC_ID}
                    AND ne{i}.{CLASS_ID} = ?
                """

                if not include_errors:
                    query += f" AND ne{i}.{ERROR_ID} IS NULL"
                if not include_overlaps:
                    query += f" AND ne{i}.{NE_OVERLAP} IS 0"

                query += ")"
                params.append(ne_id)

            # For each excluded class, add a NOT EXISTS subquery
            for i, ne_id in enumerate(excluded_ne_ids):
                query += f"""
                {'WHERE' if not params else 'AND'} NOT EXISTS (
                    SELECT 1 FROM {TABLE_NE} ne_ex{i}
                    WHERE ne_ex{i}.{DOC_ID} = d.{DOC_ID}
                    AND ne_ex{i}.{CLASS_ID} = ?
                """

                if not include_errors:
                    query += f" AND ne_ex{i}.{ERROR_ID} IS NULL"
                if not include_overlaps:
                    query += f" AND ne_ex{i}.{NE_OVERLAP} IS 0"

                query += ")"
                params.append(ne_id)

        try:
            self.logger.debug(f"Executing query: {query}")
            self.logger.debug(f"Parameters: {params}")
            self.cursor.execute(query, params)
            result = self.cursor.fetchone()
            count = result[0] if result is not None else 0

            if count == 0:
                self.logger.warning(
                    f"No documents with entities found."
                    f"\n- Total documents: {self.document_count:,}"
                    f"\n- Total entities in database: {self.named_entities_count}"  # Removed :, formatter
                    f"\n- Query: {query}"
                    f"\n- Parameters: {params}",
                )

            return count

        except Exception as e:
            self.logger.error(f"Error in documents_with_entities: {e}")
            self.logger.error(f"Failed query: {query}")
            self.logger.error(f"Parameters: {params}")
            return 0

    def documents_without_entities(self, ne_class: str = None):
        """Get counts of documents with and without named entities.
        If ne_class is specified, only count documents without that named entity class otherwise passes None to the the method.

        Returns:
            dict: Document counts with keys 'with_entities', 'without_entities', and 'total'

        """
        return self.document_count - self.documents_with_entities(ne_class=ne_class)

    @cached(ttl_seconds=7600)
    def get_entity_class_document_distribution(self):
        """# Todo implement with showing distribution of documents with and without entities and with many entities
        Get distribution of named entity classes across documents.

        Returns:
            DataFrame: Distribution with columns [class_name, document_count, percentage]

        """
        self.cursor.execute(
            f"""--sql
            SELECT
                nec.{NE_CLASS},
                COUNT(DISTINCT ne.{DOC_ID}) as document_count
            FROM {TABLE_NE} ne
            JOIN {TABLE_NE_CLASS} nec ON ne.{CLASS_ID} = nec.{CLASS_ID}
            JOIN sentences s ON ne.{SENT_IDX} = s.{SENT_IDX}
            GROUP BY nec.{NE_CLASS}
            ORDER BY document_count DESC
        """,
        )

        results = self.cursor.fetchall()
        df = pd.DataFrame(results, columns=[NE_CLASS, "document_count"])

        # Add total documents row with class_name None

        # Calculate percentage of total documents
        total_docs = self.document_count
        df["percentage"] = (df["document_count"] / total_docs * 100).round(2)

        return df

    @cached(ttl_seconds=3600)
    def get_documents_with_entity_class_combinations(self, top_n=None):
        """Get counts of documents with specific entity class combinations.

        Args:
            top_n (int, optional): Limit to top N combinations by document count

        Returns:
            DataFrame: Combinations with columns for each class (True/False) and document_count

        """
        # First get all entity classes
        self.cursor.execute(f"SELECT class_name FROM {TABLE_NE_CLASS}")
        classes = [row[0] for row in self.cursor.fetchall()]

        # Get document-class pairs
        self.cursor.execute(
            f"""
            SELECT
                s.{DOC_ID},
                nec.{NE_CLASS}
            FROM {TABLE_NE} ne
            JOIN {TABLE_NE_CLASS} nec ON ne.{CLASS_ID} = nec.{CLASS_ID}
            JOIN {TABLE_SENTENCES} s ON ne.{SENT_IDX} = s.{SENT_IDX}
            GROUP BY s.{DOC_ID}, nec.{NE_CLASS}
        """,
        )

        # Process results to get combinations
        doc_classes = {}
        for doc_id, class_name in self.cursor.fetchall():
            if doc_id not in doc_classes:
                doc_classes[doc_id] = set()
            doc_classes[doc_id].add(class_name)

        # Count combinations
        combinations = {}
        for doc_id, class_set in doc_classes.items():
            key = tuple(sorted(class_set))
            combinations[key] = combinations.get(key, 0) + 1

        # Convert to DataFrame
        rows = []
        for classes_tuple, count in sorted(combinations.items(), key=lambda x: -x[1]):
            row = {cls: cls in classes_tuple for cls in classes}
            row["document_count"] = count
            rows.append(row)

        df = pd.DataFrame(rows)

        # Limit to top N combinations if specified
        if top_n is not None and top_n > 0:
            df = df.head(top_n)

        return df

    def count_named_entity_errors(self) -> DataFrame:
        """Counts the frequency of each error type (error_id) for each named entity from the entity_occurrences table.
        Also collects the fq of the named entity from the named_entities table.
        """
        self.logger.info("Counting named entity error frequencies...")
        self.cursor.execute(
            f"""--sql
            SELECT
                {NE_CLASS} as {NE_CLASS.lower()},
                {ERROR_ID} as {ERROR_ID.lower()},
                COUNT(*) as error_count
            FROM {VIEW_NE_COMP}
            WHERE {ERROR_ID} IS NOT NULL
            GROUP BY {ERROR_ID}, {NE_CLASS}
            ORDER BY error_count DESC
            """,
        )
        error_counts = self.cursor.fetchall()
        error_df = DataFrame(
            error_counts,
            columns=["named_entity_class", "error_id", "error_count"],
        )

        self.logger.info(f"Counted named entity errors: {len(error_df)} records found.")
        return error_df

    @cached(ttl_seconds=3600)
    def results_entity_occurrence_errors(self) -> DataFrame:
        """Aggregates error_df from count_named_entity_errors and combines with the fq count for each named entity from TABLE named_entities
        Columns are named_entity, fq, error_id[1], error_id[2], ..., error_id[n]
        Rows: text for each named entity with fq and error counts
        Where error_ids are the list of unique error_ids in named_entity_errors.
        """
        error_df = self.count_named_entity_errors()

        # Fetch fq data from named_entities table
        total_fq_dis = self.named_entities_count(
            "DIS",
            include_errors=False,
            include_overlaps=False,
        )
        total_fq_pnm = self.named_entities_count(
            "PNM",
            include_errors=False,
            include_overlaps=False,
        )
        dis_fq = self.named_entities_count(
            "DIS",
            include_errors=True,
            include_overlaps=True,
        )
        pnm_fq = self.named_entities_count(
            "PNM",
            include_errors=True,
            include_overlaps=True,
        )

        fq_data = [
            ("DIS", dis_fq),
            ("PNM", pnm_fq),
        ]

        fq_with_errors = [
            ("DIS", total_fq_dis),
            ("PNM", total_fq_pnm),
        ]
        fq_df = DataFrame(fq_data, columns=["named_entity_class", "fq"])
        fq_with_errors_df = DataFrame(
            fq_with_errors,
            columns=["named_entity_class", "fq"],
        )

        # Create DataFrame for fq data including total fq counts with include_errors=True, include_overlaps=True
        fq_combined_df = pd.concat([fq_df, fq_with_errors_df], ignore_index=True)

        # Pivot the error DataFrame to have error_ids as columns
        pivot_df = (
            error_df.pivot(
                index="named_entity_class",
                columns="error_id",
                values="error_count",
            )
            .fillna(0)
            .astype(int)
        )
        pivot_df.reset_index(inplace=True)

        # Merge with fq data
        result_df = pivot_df.merge(fq_df, on="named_entity_class", how="left")
        result_df = pivot_df.merge(
            fq_with_errors_df,
            on="named_entity_class",
            how="left",
            suffixes=("", "_total"),
        )

        # Get overlap counts
        with_overlap_counts = self.named_entities_count(include_overlaps=True)
        result_df["overlap_count"] = with_overlap_counts - result_df["fq"]
        result_df["overlap_count"] = result_df["overlap_count"].clip(lower=0)
        result_df["overlap_percentage"] = (
            result_df["overlap_count"] / result_df["fq"] * 100
        ).round(2)

        # Reorder columns to place 'fq' right after 'named_entity'
        error_id_columns = [
            col for col in pivot_df.columns if col != "named_entity_class"
        ]
        columns_order = ["named_entity_class", "fq"] + error_id_columns
        result_df = result_df[columns_order]

        return result_df

    def results_dis_pnm_cooccurrences(
        self,
        sort_by: str = "pmi",
        ascending: bool = False,
        rows: int | None = None,
    ) -> None:
        """Write disease-phenomenon co-occurrences to a CSV file using the view_disease_phenomena_summary.

        Args:
            sort_by: Column to sort by. One of ['pmi', 'fq_document_level', 'fq_sentence_level']
            ascending: Sort order, False for descending, True for ascending
            rows: Optional number of rows to limit the result

        """
        valid_sort_columns = ["pmi", "fq_document_level", "fq_sentence_level"]
        if sort_by not in valid_sort_columns:
            msg = f"sort_by must be one of {valid_sort_columns}"
            raise ValueError(msg)

        order_dir = "ASC" if ascending else "DESC"
        limit_clause = f"LIMIT {rows}" if rows else ""

        query = f"""
        SELECT disease, phenomenon, fq_document_level, fq_sentence_level, pmi
        FROM view_disease_phenomena_summary
        ORDER BY {sort_by} {order_dir}
        {limit_clause}
        """

        results_file = os.path.join(self._results_dir, "dis_pnm_cooccurrences.csv")
        self.logger.info(f"Writing dis-pnm co-occurrences to {results_file}")

        try:
            self.cursor.execute(query)
            with open(results_file, "w") as f:
                header = [
                    "disease",
                    "phenomenon",
                    "doc_frequency",
                    "sent_frequency",
                    "pmi",
                ]
                f.write(",".join(header) + "\n")

                for row in self.cursor:
                    formatted_row = [str(x) if x is not None else "" for x in row]
                    f.write(",".join(formatted_row) + "\n")

            self.logger.info(f"Successfully wrote results to {results_file}")

        except Exception as e:
            self.logger.error(f"Error writing dis-pnm co-occurrences: {e}")
            raise

    def suite_results(self) -> None:
        """Output all results to CSV files."""
        # Create results directory if it does not exist
        if not os.path.exists(self._results_dir):
            os.makedirs(self._results_dir)

        # Export results to CSV files
        error_df = self.results_entity_occurrence_errors()
        self._export_df_(error_df, "entity_occurrence_statistics.csv")

    @property
    @cached(prefix="stats.processed_file_count")
    def processed_file_count(self):
        """Get the total number of processed files in the database.

        :return: The number of processed files.
        """
        self.cursor.execute("SELECT COUNT(*) FROM processed_files;")
        return self.cursor.fetchone()[0]

    def get_raw_cooccurrence_counts(self) -> None:
        """Get the total number of raw cooccurrences in the database.

        :return: The number of raw cooccurrences.
        """
        self.cursor.execute(
            """
            SELECT
                COUNT(*) as total_pairs,
                COUNT(CASE WHEN fq_document_level IS NOT NULL THEN 1 END) as doc_level_pairs,
                COUNT(CASE WHEN fq_sentence_level IS NOT NULL THEN 1 END) as sent_level_pairs,
                COUNT(CASE WHEN fq_document_level IS NOT NULL AND fq_sentence_level IS NOT NULL THEN 1 END) as both_levels
            FROM entity_cooccurrences_summary
        """,
        )
        stats = self.cursor.fetchone()
        self.logger.info(
            f"Entity co-occurrences summarization complete:"
            f"\n- Total unique entity pairs: {stats[0]:,}"
            f"\n- Document-level pairs: {stats[1]:,}"
            f"\n- Sentence-level pairs: {stats[2]:,}"
            f"\n- Pairs at both levels: {stats[3]:,}",
        )

    def plot_document_sentence_count_distribution(self) -> None:
        all_counts = []

        for chunk in pd.read_sql_query(
            "SELECT document_id, COUNT(*) AS sentence_count FROM sentences GROUP BY document_id",
            self.conn,
            chunksize=100000,
        ):
            all_counts.extend(chunk["sentence_count"].tolist())

        # Plotting with seaborn (using the accumulated list)
        sns.histplot(
            all_counts,
            bins=20,
            kde=True,
        )  # or sns.countplot(x=all_counts) if you have a smaller number of distinct sentence counts

        plt.xlabel("Sentence Count")
        plt.ylabel("Frequency")
        plt.title("Distribution of Document Sentence Counts")
        plt.tight_layout()

        plot_file = os.path.join(
            self._results_dir,
            "document_sentence_count_distribution.png",
        )
        plt.savefig(plot_file)
        plt.close()
        self.logger.info(
            f"Document sentence count distribution plot saved to {plot_file}",
        )

    def plot_document_word_count_distribution(self) -> None:
        """Plot the distribution of abstract word counts using seaborn."""
        self.cursor.execute("SELECT word_count FROM documents;")
        word_counts = [row[0] for row in self.cursor.fetchall()]
        sns.histplot(word_counts, bins=20, kde=True)
        plt.xlabel("Word Count")
        plt.ylabel("Frequency")
        plt.title("Distribution of Abstract Word Counts")
        plt.tight_layout()

        # Save plot
        plot_file = os.path.join(
            self._results_dir,
            "document_word_count_distribution.png",
        )
        plt.savefig(plot_file)
        plt.close()
        self.logger.info(f"Document word count distribution plot saved to {plot_file}")

    def plot_document_distributions(self) -> None:
        """Plot the distribution of both sentence counts and word counts per document in a single figure with two subplots."""
        # Create figure with two subplots side by side
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Plot sentence count distribution
        all_counts = []
        for chunk in pd.read_sql_query(
            "SELECT document_id, COUNT(*) AS sentence_count FROM sentences GROUP BY document_id",
            self.conn,
            chunksize=100000,
        ):
            all_counts.extend(chunk["sentence_count"].tolist())

        sns.histplot(all_counts, bins=20, kde=True, ax=ax1)
        ax1.set_xlabel("Sentences per Document")
        ax1.set_ylabel("Frequency")
        ax1.set_title("Distribution of Document Sentence Counts")

        # Plot word count distribution
        self.cursor.execute("SELECT word_count FROM documents;")
        word_counts = [row[0] for row in self.cursor.fetchall()]
        sns.histplot(word_counts, bins=20, kde=True, ax=ax2)
        ax2.set_xlabel("Words per Document")
        ax2.set_ylabel("Frequency")
        ax2.set_title("Distribution of Document Word Counts")

        # Adjust layout and save
        plt.tight_layout()
        plot_file = os.path.join(self._results_dir, "document_distributions.png")
        plt.savefig(plot_file)
        plt.close()
        self.logger.info(f"Document distributions plot saved to {plot_file}")

    def export_overview_table(self) -> None:
        """Get an overview of the tables in the database."""
        df = pd.DataFrame(
            {
                "Table": [
                    "documents",
                    "sentences",
                    "entity_occurrences",
                    "entity_cooccurrences",
                ],
                "Count": [
                    self.document_count,
                    self.sentence_count,
                    self.named_entities_count,
                    self.get_entity_cooccurrence_count,
                ],
            },
        )

        self._export_df_(df, "database_overview.csv")

    def cooccurrence_stats(self, level: str = "document") -> None:
        """Get statistics on entity co-occurrences."""
        # Retrieve and log final co-occurrence statistics
        query = f"""
            SELECT
                COUNT(*) as total_pairs,
                (SELECT COUNT(DISTINCT entity_id)
                FROM entity_occurrences
                WHERE id IN (SELECT e1_id FROM entity_cooccurrences UNION SELECT e2_id FROM entity_cooccurrences)) as total_entities,
                {"COALESCE(AVG(sentence_distance), 0) as avg_distance " if level == "sentence" else ""}
            FROM entity_cooccurrences
            """

        self.cursor.execute(query)

        total_stats = self.cursor.fetchone()
        self.logger.info(
            f"\nCo-occurrence identification complete:"
            f"\n- Total unique pairs: {total_stats[0]:,}"
            f"\n- Unique entities involved: {total_stats[1]:,}"
            + (
                f"\n- Average sentence distance: {total_stats[2]:.2f}"
                if level == "sentence"
                else ""
            ),
        )
