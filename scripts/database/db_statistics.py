class DBStatistics:
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
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024 or unit == 'TB':
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

    def get_document_count(self):
        """
        Get the total number of documents in the database.

        :return: The number of documents.
        """
        self.cursor.execute("SELECT COUNT(*) FROM documents;")
        return self.cursor.fetchone()[0]

    def get_sentence_count(self):
        """
        Get the total number of sentences in the database.

        :return: The number of sentences.
        """
        self.cursor.execute("SELECT COUNT(*) FROM sentences;")
        return self.cursor.fetchone()[0]

    def get_named_entity_count(self):
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

    def get_processed_file_count(self):
        """
        Get the total number of processed files in the database.

        :return: The number of processed files.
        """
        self.cursor.execute("SELECT COUNT(*) FROM processed_files;")
        return self.cursor.fetchone()[0]
