class DBDataExchanger:

    @property
    def source_files(self):
        """
        Get dictionary of source files in the database.
        """
        self.cursor.execute("SELECT * FROM source_files;")
        return self.cursor.fetchall()

    def get_entity_occurrence(self, eo_id):
        """
        Get entity occurrences by entity ID.
        """
        self.cursor.execute(
            "SELECT * FROM entity_occurrences WHERE id = ?", (eo_id,)
        )
        return self.cursor.fetchone()
    

    def get_document_as_html(doc_id):
        """
        Get document as HTML. Entity spans can overlap, thus a single <span> cannot be used to highlight multiple overlapping entities. Instead we create overlays in the correct positions of the text and use CSS to style them.
        """
        self.cursor.execute(
            """
            
            """
        )
        return self.cursor.fetchall()
    
    def get_sentences(self, doc_id):
        """
        Get sentences by document ID.
        """
        self.cursor.execute(
            "SELECT * FROM sentences WHERE document_id = ?", (doc_id,)
        )
        return self.cursor.fetchall()
    
    