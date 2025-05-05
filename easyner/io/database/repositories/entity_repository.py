import pandas as pd
from typing import Dict, List, Any, Union, Optional
import logging
import warnings

from easyner.io.database.utils.transaction import transactional

from .base import Repository
from ..connection import DatabaseConnection


class EntityRepository(Repository):
    """
    Repository for managing entity data in the database.

    Provides methods to retrieve, insert, and query entity records.
    """

    def __init__(self, connection: DatabaseConnection):
        """
        Initialize EntityRepository.

        Args:
            connection: Database connection object
        """
        self.logger = logging.getLogger(__name__)
        self.connection = connection

    def get_all(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all entities from the database.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of entity dictionaries
        """
        if as_df:
            return self.get_all_df()
        else:
            return self.get_all_dict_list()

    def get_all_df(self) -> pd.DataFrame:
        """
        Get all entities from the database as a DataFrame.

        Returns:
            DataFrame containing entity data
        """
        try:
            query = """
                SELECT article_id, sentence_id, entity, start_pos, end_pos,
                    inference_model, inference_model_metadata
                FROM entities
            """
            result = self.connection.execute(query)
            return result.fetchdf()
        except Exception as e:
            self.logger.error(f"Error retrieving entities as DataFrame: {e}")
            raise

    def get_all_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all entities from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing entity data
        """
        try:
            query = """
                SELECT article_id, sentence_id, entity, start_pos, end_pos,
                    inference_model, inference_model_metadata
                FROM entities
            """
            rows = self.connection.execute(query).fetchall()
            return [
                {
                    "article_id": row[0],
                    "sentence_id": row[1],
                    "entity": row[2],
                    "start_pos": row[3],
                    "end_pos": row[4],
                    "inference_model": row[5],
                    "inference_model_metadata": row[6],
                }
                for row in rows
            ]
        except Exception as e:
            self.logger.error(
                f"Error retrieving entities as dictionary list: {e}"
            )
            raise

    def get_by_article_id(
        self, article_id: int, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get entities for a specific article.

        Args:
            article_id: Article ID to filter by
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of entity dictionaries for the specified article
        """
        try:
            query = """
                SELECT article_id, sentence_id, entity, start_pos, end_pos,
                    inference_model, inference_model_metadata
                FROM entities
                WHERE article_id = ?
            """
            result = self.connection.execute(query, [article_id])

            if as_df:
                return result.fetchdf()
            else:
                rows = result.fetchall()
                return [
                    {
                        "article_id": row[0],
                        "sentence_id": row[1],
                        "entity": row[2],
                        "start_pos": row[3],
                        "end_pos": row[4],
                        "inference_model": row[5],
                        "inference_model_metadata": row[6],
                    }
                    for row in rows
                ]
        except Exception as e:
            self.logger.error(f"Error retrieving entities by article ID: {e}")
            raise

    def get_by_sentence(
        self, article_id: int, sentence_id: int, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get entities for a specific sentence.

        Args:
            article_id: Article ID
            sentence_id: Sentence ID
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of entity dictionaries for the specified sentence
        """
        try:
            query = """
                SELECT article_id, sentence_id, entity, start_pos, end_pos,
                    inference_model, inference_model_metadata
                FROM entities
                WHERE article_id = ? AND sentence_id = ?
            """
            result = self.connection.execute(query, [article_id, sentence_id])

            if as_df:
                return result.fetchdf()
            else:
                rows = result.fetchall()
                return [
                    {
                        "article_id": row[0],
                        "sentence_id": row[1],
                        "entity": row[2],
                        "start_pos": row[3],
                        "end_pos": row[4],
                        "inference_model": row[5],
                        "inference_model_metadata": row[6],
                    }
                    for row in rows
                ]
        except Exception as e:
            self.logger.error(f"Error retrieving entities by sentence: {e}")
            raise

    def insert(self, entity: Dict[str, Any]) -> None:
        """
        Insert an entity into the database.

        Args:
            entity: Entity data as a dictionary with required entity fields
        """
        try:
            query = """
                INSERT INTO entities (article_id, sentence_id, entity, start_pos, end_pos,
                                     inference_model, inference_model_metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.connection.execute(
                query,
                [
                    entity["article_id"],
                    entity["sentence_id"],
                    entity["entity"],
                    entity["start_pos"],
                    entity["end_pos"],
                    entity.get("inference_model"),  # Optional fields
                    entity.get("inference_model_metadata"),
                ],
            )
        except Exception as e:
            self.logger.error(f"Error inserting entity: {e}")
            raise

    @transactional
    def insert_many(
        self, entities: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple entities into the database.

        Args:
            entities: List of entity dictionaries or DataFrame containing entity data
        """
        try:
            # If given a DataFrame, register it as a view
            if isinstance(entities, pd.DataFrame):
                self.connection.register("entities_df", entities)
                self.connection.execute(
                    """
                    INSERT INTO entities (article_id, sentence_id, entity, start_pos, end_pos,
                                         inference_model, inference_model_metadata)
                    SELECT article_id, sentence_id, entity, start_pos, end_pos,
                           inference_model, inference_model_metadata
                    FROM entities_df
                    """
                )
            else:
                # For list of dictionaries, process each one
                for entity in entities:
                    self.insert(entity)
        except Exception as e:
            self.logger.error(f"Error batch inserting entities: {e}")
            raise

    def get_entity_stats(self) -> pd.DataFrame:
        """
        Get statistics about entities per article and sentence.

        Returns:
            DataFrame with entity statistics by article
        """
        try:
            query = """
                SELECT a.article_id, a.title,
                       COUNT(DISTINCT s.sentence_id) AS sentence_count,
                       COUNT(e.entity) AS entity_count
                FROM articles a
                LEFT JOIN sentences s ON a.article_id = s.article_id
                LEFT JOIN entities e ON s.article_id = e.article_id AND s.sentence_id = e.sentence_id
                GROUP BY a.article_id, a.title
                ORDER BY entity_count DESC
            """
            return self.connection.execute(query).fetchdf()
        except Exception as e:
            self.logger.error(f"Error getting entity statistics: {e}")
            raise
