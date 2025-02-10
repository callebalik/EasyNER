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
