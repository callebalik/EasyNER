from typing import List, Dict, Any
import sqlite3
from data_model import Document, Sentence, NamedEntity
import logging


class DBDataExchanger:

    def __init__(
        self, conn: sqlite3.Connection, cursor: sqlite3.Cursor, logger: logging.Logger
    ):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        

    def bulk_insert(self, insert_query, data):
        """
        Inserts many rows into the database in a single transaction.

        :param insert_query: SQL insert statement with placeholders.
        :param data: An iterable of tuples containing the data rows.
        """
        try:
            self.conn.execute("BEGIN TRANSACTION;")
            self.cursor.executemany(insert_query, data)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    @property
    def source_files(self):
        """
        Get dictionary of source files in the database.
        """
        try:
            self.cursor.execute("SELECT * FROM source_files;")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching source files: {e}")
            return []

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Helper function to convert a sqlite3.Row object to a dictionary."""
        if row:
            return dict(zip(row.keys(), row))
        return None

    def get_document(self, doc_id: int) -> Document:
        with self.conn:
            self.conn.row_factory = sqlite3.Row  # Configure the connection to return sqlite3.Row objects
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT * FROM documents WHERE id = ?", (doc_id,))
                document_row = cursor.fetchone()
                document_dict = self._row_to_dict(document_row)
                if not document_dict:
                    return None

                sentences = self.get_sentences(doc_id)
                document_dict["sentences"] = sentences
                return Document(**document_dict)
            except sqlite3.Error as e:
                self.logger.error(f"Error fetching document {doc_id}: {e}")
                return None

    def get_sentences(self, doc_id: int) -> List[Sentence]:
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM sentences WHERE document_id = ?", (doc_id,))
            sentences = cursor.fetchall()
            sentence_objects = []
            for sentence_row in sentences:
                sentence_dict = self._row_to_dict(sentence_row)
                entities = self.get_entities(doc_id, sentence_dict["sentence_index"])
                sentence_dict["entities"] = entities
                sentence_objects.append(Sentence(**sentence_dict))
            return sentence_objects
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching sentences for document {doc_id}: {e}")
            return []

    def get_entities(self, doc_id: int, sentence_index: int) -> List[NamedEntity]:
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT * FROM entity_occurrences 
                WHERE document_id = ? AND sentence_index = ?
                """,
                (doc_id, sentence_index),
            )
            entities = cursor.fetchall()
            return [NamedEntity(**self._row_to_dict(entity)) for entity in entities]
        except sqlite3.Error as e:
            self.logger.error(
                f"Error fetching entities for document {doc_id}, sentence {sentence_index}: {e}"
            )
            return []
    
    def get_named_entity_id(self, named_entity: str) -> int:
        try:
            self.cursor.execute(
                "SELECT id FROM named_entities WHERE named_entity = ?", (named_entity,)
            )
            result = self.cursor.fetchone()
            if result:
                return result[0]
            else:
                self.logger.warning(f"No ID found for named entity: {named_entity}")
                return None  # Or raise an exception if appropriate
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching named entity ID for {named_entity}: {e}")
            return None
        
    def search_entities(
        self,
        type: str = None,
        doc_id: int = None,
        sentence_index: int = None,
        like: str = None,
    ):
        """
        Build and execute a query filtering by optional parameters.
        """
        query = "SELECT * FROM entity_occurrences"
        conditions = []
        params = []
        if type is not None:
            conditions.append("entity_id = ?")
            params.append(self.get_named_entity_id(type))
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

        try:
            self.cursor.execute(query, params)
            # Fetch column names from cursor description
            columns = [col[0] for col in self.cursor.description]
            # Convert rows to dictionaries
            rows = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            return rows
        except sqlite3.Error as e:
            self.logger.error(f"Error searching entities: {e}")
            return []

    def get_entity_occurrence(self, eo_id):
        """
        Get entity occurrences by entity ID.
        """
        try:
            self.cursor.execute(
                "SELECT * FROM entity_occurrences WHERE id = ?", (eo_id,)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching entity occurrence {eo_id}: {e}")
            return None
