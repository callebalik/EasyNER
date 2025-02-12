# from data_model import document, sentences, entities

from typing import List, Dict, Any
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
    
        
    def get_document(self, doc_id: int) -> Dict[str, Any]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM documents WHERE id = ?", (doc_id,))
        document = cursor.fetchone()
        return dict(document) if document else None

    def get_sentences(self, doc_id: int) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM sentences WHERE document_id = ?", (doc_id,))
        sentences = cursor.fetchall()
        return [dict(sentence) for sentence in sentences]

    def get_entities(self, doc_id: int, sentence_index: int) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM entity_occurrences 
            WHERE document_id = ? AND sentence_index = ?
        """, (doc_id, sentence_index))
        entities = cursor.fetchall()
        return [dict(entity) for entity in entities]

    def get_document_as_html(self, doc_id: int) -> str:
        document = self.get_document(doc_id)
        if not document:
            return ""

        sentences = self.get_sentences(doc_id)
        html_content = f"<h1>{document['title']}</h1>"

        for sentence in sentences:
            entities = self.get_entities(doc_id, sentence['sentence_index'])
            sentence_text = sentence['text']
            for entity in entities:
                entity_text = entity['entity_text']
                sentence_text = sentence_text.replace(entity_text, f"<b>{entity_text}</b>")
            html_content += f"<p>{sentence_text}</p>"

        return html_content


    def get_document_details(self, doc_id):
        """
        Get document details including title and basic metrics.
        """
        # Debug log the query
        query = "SELECT id, title, word_count FROM documents WHERE id = ?"
        self.logger.debug(f"Executing query: {query} with doc_id: {doc_id}")
        
        try:
            # Use direct cursor execution for better error tracking
            self.cursor.execute(query, (doc_id,))
            row = self.cursor.fetchone()
            
            if row:
                # Convert tuple to dict manually to ensure correct mapping
                result = {
                    'id': row[0],
                    'title': row[1],
                    'word_count': row[2]
                }
                self.logger.info(f"Retrieved document {doc_id}: {result['title']}")
                return result
            else:
                # Log the full query result for debugging
                self.logger.warning(f"Document {doc_id} not found. Query returned no results.")
                return None
                
        except Exception as e:
            self.logger.error(f"Error retrieving document {doc_id}: {e}")
            raise

    def get_sentences(self, doc_id):
        """
        Get sentences by document ID.
        """
        self.cursor.execute(
            "SELECT * FROM sentences WHERE document_id = ?", (doc_id,)
        )
        return self.cursor.fetchall()
    
    def get_entities(self, doc_id: int = None, sentence_index: int = None, like: str = None):
        """
        Build and execute a query filtering by optional parameters.
        """
        query = "SELECT * FROM entity_occurrences"
        conditions = []
        params = []
    
        if doc_id is not None:
            conditions.append("document_id = ?")
            params.append(doc_id)
        if sentence_index is not None:
            conditions.append("sentence_index = ?")
            params.append(sentence_index)
        if like is not None:
            conditions.append("entity_text LIKE ?")
            params.append(f"%{like}%")
    
        if conditions:
            query += " WHERE " + " OR ".join(conditions)
    
        self.cursor.execute(query, params)
        return self.cursor.fetchall()    
    

    def save_docs_as_json(doc_ids : list[int], path) -> None:
        pass
    
    def export_doc_as_json(doc_id) -> str:
        pass


  