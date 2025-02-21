from typing import List, Dict, Any
import sqlite3
from .data_model.data_model import Document, Sentence, NamedEntity
import logging


class DBDataExchanger:

    def __init__(
        self, conn: sqlite3.Connection, cursor: sqlite3.Cursor, logger: logging.Logger
    ):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger

    def _safe_count(self, table_name: str) -> int:
        """Get count with zero-value protection"""
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        result = self.cursor.fetchone()[0]
        return result if result > 0 else 1

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
            self.conn.row_factory = (
                sqlite3.Row
            )  # Configure the connection to return sqlite3.Row objects
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
            SELECT 
                veo.id,
                veo.entity_text,
                veo.named_entity,
                veo.normalized_entity_text,
                veo.document_id,
                eo.sentence_index,
                eos.span_start,
                eos.span_end
            FROM view_entity_occurrences veo
            JOIN entity_occurrences eo ON veo.id = eo.id
            LEFT JOIN entity_occurrence_spans eos ON veo.id = eos.id
            WHERE veo.document_id = ? AND eo.sentence_index = ?
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
        sort_by: str = "tf_idf",
        sort_order: str = "desc",
    ):
        """
        Build and execute a query filtering by optional parameters.

        Args:
            type: Filter by entity type
            doc_id: Filter by document ID
            sentence_index: Filter by sentence index
            like: Search entity text using LIKE
            sort_by: Column to sort by
            sort_order: Sort direction ('asc' or 'desc')
        """
        query = """
            SELECT eo.*, ne.named_entity 
            FROM entity_occurrences eo
            JOIN named_entities ne ON ne.id = eo.entity_id
            WHERE 1=1
        """
        params = []

        if type is not None:
            query += " AND entity_id = ?"
            params.append(type)
        if doc_id is not None:
            query += " AND document_id = ?"
            params.append(doc_id)
        if sentence_index is not None:
            query += " AND sentence_index = ?"
            params.append(sentence_index)
        if like is not None:
            query += " AND entity_text LIKE ?"
            params.append(f"%{like}%")

        # Map front-end sort columns to actual database columns
        sort_columns = {
            "entity_text": "eo.entity_text",
            "named_entity": "ne.named_entity",
            "document_id": "eo.document_id",
            "sentence_index": "eo.sentence_index",
            "tf_idf": "eo.tf_idf",
            "intra_doc_fq": "eo.intra_doc_fq",
        }

        # Add ORDER BY clause using the mapped column
        sort_column = sort_columns.get(sort_by, "eo.tf_idf")
        sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
        query += f" ORDER BY {sort_column} {sort_direction}"

        try:
            self.cursor.execute(query, params)
            columns = [col[0] for col in self.cursor.description]
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

    def get_entity_cooccurrences(self, e1_id: int, e2_id: int, level: str = "document"):
        pass

    def get_cooccurrences_summary(
        self,
        page: int = 1,
        per_page: int = 30,
        include_self: bool = False,
        entity1_type: str = None,
        entity2_type: str = None,
        sort: str = "fq_document_level",
        order: str = "desc",
        entity1_search: str = None,
        entity2_search: str = None,
    ):
        """
        Get entity co-occurrences summary with entity texts from entity_occurrences.

        Args:
            page: Page number (1-based)
            per_page: Number of records per page
            include_self: Whether to include self-cooccurrences
            entity1_type: Filter by first entity type ID
            entity2_type: Filter by second entity type ID
            sort: Column to sort by
            order: Sort direction ('asc' or 'desc')
            entity1_search: Search term for entity 1
            entity2_search: Search term for entity 2

        Returns:
            dict: Contains summaries list, has_more flag, and total count
        """
        offset = (page - 1) * per_page
        try:
            where_clauses = []
            params = []

            if not include_self:
                where_clauses.append("ecs.e1_id_normalized != ecs.e2_id_normalized")

            if entity1_type:
                where_clauses.append(
                    "ecs.e1_id_normalized IN (SELECT id FROM entity_occurrences WHERE entity_id = ?)"
                )
                params.append(entity1_type)
            if entity2_type:
                where_clauses.append(
                    "ecs.e2_id_normalized IN (SELECT id FROM entity_occurrences WHERE entity_id = ?)"
                )
                params.append(entity2_type)

            # Add entity text search conditions
            if entity1_search:
                where_clauses.append("e1.entity_text LIKE ?")
                params.append(f"%{entity1_search}%")
            if entity2_search:
                where_clauses.append("e2.entity_text LIKE ?")
                params.append(f"%{entity2_search}%")

            # Get total count for pagination
            count_sql = """
                SELECT COUNT(*) 
                FROM entity_cooccurrences_summary ecs
                JOIN entity_occurrences e1 ON ecs.e1_id_normalized = e1.id
                JOIN entity_occurrences e2 ON ecs.e2_id_normalized = e2.id
                WHERE 1=1
            """

            if where_clauses:
                count_sql += " AND " + " AND ".join(where_clauses)

            self.cursor.execute(count_sql, params)
            total_count = self.cursor.fetchone()[0]
            self.logger.info(f"Total co-occurrences summary records: {total_count}")

            if total_count == 0:
                return {"summaries": [], "has_more": False, "total": 0}

            # Map front-end sort columns to actual database columns
            sort_columns = {
                "entity1_text": "e1.entity_text",
                "entity2_text": "e2.entity_text",
                "fq_document_level": "ecs.fq_document_level",
                "fq_document_level_normalized": "ecs.fq_document_level_normalized",
                "fq_sentence_level": "ecs.fq_sentence_level",
                "fq_sentence_level_normalized": "ecs.fq_sentence_level_normalized",
            }

            # Main query with joins to get entity texts
            sql = """
                SELECT 
                    ecs.id,
                    ecs.e1_id_normalized,
                    ecs.e2_id_normalized,
                    e1.entity_text as entity1_text,
                    e2.entity_text as entity2_text,
                    ecs.fq_document_level,
                    ecs.fq_document_level_normalized,
                    ecs.fq_sentence_level,
                    ecs.fq_sentence_level_normalized,
                    ne1.named_entity as entity1_type,
                    ne2.named_entity as entity2_type
                FROM entity_cooccurrences_summary ecs
                JOIN entity_occurrences e1 ON ecs.e1_id_normalized = e1.id
                JOIN entity_occurrences e2 ON ecs.e2_id_normalized = e2.id
                JOIN named_entities ne1 ON e1.entity_id = ne1.id
                JOIN named_entities ne2 ON e2.entity_id = ne2.id
            """

            # Add WHERE clauses
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            # Add ORDER BY clause using the mapped column
            sort_column = sort_columns.get(sort, "ecs.fq_document_level")
            sort_direction = "DESC" if order.lower() == "desc" else "ASC"
            sql += f" ORDER BY {sort_column} {sort_direction} NULLS LAST"

            sql += " LIMIT ? OFFSET ?"

            # Add pagination parameters
            params.extend([per_page + 1, offset])

            self.cursor.execute(sql, params)
            columns = [col[0] for col in self.cursor.description]
            rows = self.cursor.fetchall()

            # Handle pagination
            has_more = len(rows) > per_page
            rows = rows[:per_page]  # Trim the extra item we fetched

            # Convert rows to dictionaries
            summaries = [dict(zip(columns, row)) for row in rows]

            self.logger.info(f"Found {len(summaries)} co-occurrence summaries")
            if summaries:
                self.logger.debug(f"Sample row: {summaries[0]}")

            return {"summaries": summaries, "has_more": has_more, "total": total_count}
        except sqlite3.Error as e:
            self.logger.error(f"Error fetching co-occurrences summary: {e}")
            return {"summaries": [], "has_more": False, "total": 0}

    def rename_named_entity(self, old_name: str, new_name: str):
        try:
            self.cursor.execute(
                "UPDATE named_entities SET named_entity = ? WHERE named_entity = ?",
                (new_name, old_name),
            )
            self.conn.commit()
            self.logger.info(f"Renamed named entity from {old_name} to {new_name}")
        except sqlite3.Error as e:
            self.conn.rollback()
            self.logger.error(
                f"Error renaming named entity from {old_name} to {new_name}: {e}"
            )
