import sqlite3
import logging
import os

from pandas import DataFrame

from db_data_exchanger import DBDataExchanger # Import pandas


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
            os.path.dirname(__file__), "..", "..", "results"
        )
        self.data_exchanger = data_exchanger

    def _export_df_(self, results: DataFrame, filename, overwrite: bool=True):
        """
        Export results to a CSV file.

        :param results: The results to export.
        :param filename: The file to export the results to.
        """
        filename = os.path.join(self._results_dir, filename)
        if os.path.exists(filename) and not overwrite:
            self.logger.error(f"File {filename} already exists. Set overwrite=True to overwrite.")
            return

        results.to_csv(filename, index=False)
        self.logger.info(f"Results exported to {filename}")

    @property
    def size(self):
        """
        Get the size of the database file in bytes.

        :return: The size of the database file in bytes.
        """
        self.cursor.execute(
            "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size();"
        )
        return self.cursor.fetchone()[0]

    @property
    def total_source_size(self):
        """
        Get the total size of the source files in bytes.
        Source files sizes are stored in MB in the database.

        :return: The total size of the source files in bytes.
        """
        self.cursor.execute("SELECT SUM(file_size) FROM source_files;")
        mb_size = self.cursor.fetchone()[0] or 0
        return mb_size * 1024 * 1024  # Convert MB to bytes

    @property
    def compression_ratio(self):
        """
        Get the compression ratio of the database file compared to source files.

        :return: The compression ratio of the database file.
        """
        source_size = self.total_source_size
        if source_size == 0:
            return 0.0
        return self.size / source_size

    def _format_size(self, size_bytes):
        """
        Convert size in bytes to human readable format.

        :param size_bytes: Size in bytes
        :return: String with formatted size
        """
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024 or unit == "TB":
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024

    @property
    def info_compression(self):
        """
        Summary of the compression ratio of the database file compared to source files.
        """
        db_size = self._format_size(self.size)
        source_size = self._format_size(self.total_source_size)  # Already in bytes
        print(f"Database size: {db_size}")
        print(f"Total source file size: {source_size}")
        print(f"Compression ratio: {self.compression_ratio:.2f}")

    @property
    def document_count(self):
        """
        Get the total number of documents in the database.

        :return: The number of documents.
        """
        self.cursor.execute("SELECT COUNT(*) FROM documents;")
        return self.cursor.fetchone()[0]

    @property
    def sentence_count(self):
        """
        Get the total number of sentences in the database.

        :return: The number of sentences.
        """
        self.cursor.execute("SELECT COUNT(*) FROM sentences;")
        return self.cursor.fetchone()[0]

    @property
    def named_entity_count(self):
        """
        Get the total number of named entities in the database.

        :return: The number of named entities.
        """
        self.cursor.execute("SELECT COUNT(*) FROM named_entities;")
        return self.cursor.fetchone()[0]

    def get_entity_occurrence_count(self):
        """
        Get the total number of entity occurrences in the database.

        :return: The number of entity occurrences.
        """
        self.cursor.execute("SELECT COUNT(*) FROM entity_occurrences;")
        return self.cursor.fetchone()[0]

    def get_entity_cooccurrence_count(self):
        """
        Get the total number of entity cooccurrences in the database.

        :return: The number of entity cooccurrences.
        """
        self.cursor.execute("SELECT COUNT(*) FROM entity_cooccurrences;")
        return self.cursor.fetchone()[0]

    def count_named_entity_errors(self) -> DataFrame:
        """
        Counts the frequency of each error type (error_id) for each named entity from the entity_occurrences table.
        Also collects the fq of the named entity from the named_entities table.
        """
        self.logger.info("Counting named entity error frequencies...")
        self.cursor.execute(
            """
            SELECT 
                ne.named_entity,
                ec.error_id,
                COUNT(*) as error_count
            FROM entity_occurrences ec
            JOIN named_entities ne ON ec.entity_id = ne.id
            WHERE ec.error_id IS NOT NULL
            GROUP BY ne.named_entity, ec.error_id
            ORDER BY ne.named_entity, error_count DESC
            """
        )
        error_counts = self.cursor.fetchall()
        error_df = DataFrame(
            error_counts, columns=["named_entity", "error_id", "error_count"]
        )
        return error_df

    def results_entity_occurrence_errors(self) -> DataFrame:
        """
        Aggregates error_df from count_named_entity_errors and combines with the fq count for each named entity from TABLE named_entities
        Columns are named_entity, fq, error_id[1], error_id[2], ..., error_id[n]
        Rows: text for each named entity with fq and error counts
        Where error_ids are the list of unique error_ids in named_entity_errors
        """
        error_df = self.count_named_entity_errors()

        # Fetch fq data from named_entities table
        self.cursor.execute("SELECT named_entity, fq FROM named_entities")
        fq_data = self.cursor.fetchall()
        fq_df = DataFrame(fq_data, columns=["named_entity", "fq"])

        # Pivot the error DataFrame to have error_ids as columns
        pivot_df = error_df.pivot(
            index="named_entity", columns="error_id", values="error_count"
        ).fillna(0).astype(int)
        pivot_df.reset_index(inplace=True)

        # Merge with fq data
        result_df = pivot_df.merge(fq_df, on="named_entity", how="left")

        # Reorder columns to place 'fq' right after 'named_entity'
        error_id_columns = [col for col in pivot_df.columns if col != "named_entity"]
        columns_order = ["named_entity", "fq"] + error_id_columns
        result_df = result_df[columns_order]

        return result_df

    def results_dis_pnm_cooccurrences(
        self, sort_by: str = "pmi", ascending: bool = False, rows: int = None
    ) -> None:
        """
        Write disease-phenomenon co-occurrences to a CSV file using the view_disease_phenomena_summary.

        Args:
            sort_by: Column to sort by. One of ['pmi', 'fq_document_level', 'fq_sentence_level']
            ascending: Sort order, False for descending, True for ascending
            rows: Optional number of rows to limit the result
        """
        valid_sort_columns = ["pmi", "fq_document_level", "fq_sentence_level"]
        if sort_by not in valid_sort_columns:
            raise ValueError(f"sort_by must be one of {valid_sort_columns}")

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
        """
        Output all results to CSV files.
        """

        # Create results directory if it does not exist
        if not os.path.exists(self._results_dir):
            os.makedirs(self._results_dir)

        # Export results to CSV files
        error_df = self.results_entity_occurrence_errors()
        self._export_df_(error_df, "entity_occurrence_statistics.csv")

    def get_processed_file_count(self):
        """
        Get the total number of processed files in the database.

        :return: The number of processed files.
        """
        self.cursor.execute("SELECT COUNT(*) FROM processed_files;")
        return self.cursor.fetchone()[0]

    def get_raw_cooccurrence_counts(self):
        """
        Get the total number of raw cooccurrences in the database.

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
        """
        )
        stats = self.cursor.fetchone()
        self.logger.info(
            f"Entity co-occurrences summarization complete:"
            f"\n- Total unique entity pairs: {stats[0]:,}"
            f"\n- Document-level pairs: {stats[1]:,}"
            f"\n- Sentence-level pairs: {stats[2]:,}"
            f"\n- Pairs at both levels: {stats[3]:,}"
        )

    def plot_document_sentence_count_distribution(self):
        all_counts = []

        for chunk in pd.read_sql_query("SELECT document_id, COUNT(*) AS sentence_count FROM sentences GROUP BY document_id", self.conn, chunksize=100000):
            all_counts.extend(chunk['sentence_count'].tolist())

        # Plotting with seaborn (using the accumulated list)
        sns.histplot(all_counts, bins=20, kde=True) # or sns.countplot(x=all_counts) if you have a smaller number of distinct sentence counts

        plt.xlabel("Sentence Count")
        plt.ylabel("Frequency")
        plt.title("Distribution of Document Sentence Counts")
        plt.tight_layout()

        plot_file = os.path.join(self._results_dir, "document_sentence_count_distribution.png")
        plt.savefig(plot_file)
        plt.close()
        self.logger.info(f"Document sentence count distribution plot saved to {plot_file}")
        
    def plot_document_word_count_distribution(self):
        """
        Plot the distribution of abstract word counts using seaborn.
        """
        self.cursor.execute("SELECT word_count FROM documents;")
        word_counts = [row[0] for row in self.cursor.fetchall()]
        sns.histplot(word_counts, bins=20, kde=True)
        plt.xlabel("Word Count")
        plt.ylabel("Frequency")
        plt.title("Distribution of Abstract Word Counts")
        plt.tight_layout()
        
        # Save plot
        plot_file = os.path.join(self._results_dir, "document_word_count_distribution.png")
        plt.savefig(plot_file)
        plt.close()
        self.logger.info(f"Document word count distribution plot saved to {plot_file}")


    def plot_document_distributions(self):
        """
        Plot the distribution of both sentence counts and word counts per document in a single figure with two subplots.
        """
        # Create figure with two subplots side by side
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Plot sentence count distribution
        all_counts = []
        for chunk in pd.read_sql_query(
            "SELECT document_id, COUNT(*) AS sentence_count FROM sentences GROUP BY document_id", 
            self.conn, 
            chunksize=100000
        ):
            all_counts.extend(chunk['sentence_count'].tolist())
        
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

    def export_overview_table(self):
        """
        Get an overview of the tables in the database.
        """
        doc_count = self.get_document_count()
        sent_count = self.get_sentence_count()
        eo_count = self.get_entity_occurrence_count()
        ec_count = self.get_entity_cooccurrence_count()

        df = pd.DataFrame(
            {
                "Table": ["documents", "sentences", "entity_occurrences", "entity_cooccurrences"],
                "Count": [doc_count, sent_count, eo_count, ec_count],
            }
        )

        self._export_df_(df, "database_overview.csv")