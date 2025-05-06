import pandas as pd
from typing import Dict, List, Any, Union, Optional
import logging
import warnings

from easyner.io.database.utils.transaction import transactional
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    SENTENCE_ID,
    TEXT,
    START_CHAR,
    END_CHAR,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
    ENTITIES_TABLE,
    ARTICLES_TABLE,  # Added for get_entity_stats
    SENTENCES_TABLE,  # Added for get_entity_stats
    TITLE,  # Added for get_entity_stats
)

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
            query = f"""
                SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                    {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA}
                FROM {ENTITIES_TABLE}
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
            query = f"""
                SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                    {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA}
                FROM {ENTITIES_TABLE}
            """
            rows = self.connection.execute(query).fetchall()
            return [
                {
                    ARTICLE_ID: row[0],
                    SENTENCE_ID: row[1],
                    TEXT: row[2],
                    START_CHAR: row[3],
                    END_CHAR: row[4],
                    INFERENCE_MODEL: row[5],
                    INFERENCE_MODEL_METADATA: row[6],
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
            query = f"""
                SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                    {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA}
                FROM {ENTITIES_TABLE}
                WHERE {ARTICLE_ID} = ?
            """
            result = self.connection.execute(query, [article_id])

            if as_df:
                return result.fetchdf()
            else:
                rows = result.fetchall()
                return [
                    {
                        ARTICLE_ID: row[0],
                        SENTENCE_ID: row[1],
                        TEXT: row[2],
                        START_CHAR: row[3],
                        END_CHAR: row[4],
                        INFERENCE_MODEL: row[5],
                        INFERENCE_MODEL_METADATA: row[6],
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
            query = f"""
                SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                    {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA}
                FROM {ENTITIES_TABLE}
                WHERE {ARTICLE_ID} = ? AND {SENTENCE_ID} = ?
            """
            result = self.connection.execute(query, [article_id, sentence_id])

            if as_df:
                return result.fetchdf()
            else:
                rows = result.fetchall()
                return [
                    {
                        ARTICLE_ID: row[0],
                        SENTENCE_ID: row[1],
                        TEXT: row[2],
                        START_CHAR: row[3],
                        END_CHAR: row[4],
                        INFERENCE_MODEL: row[5],
                        INFERENCE_MODEL_METADATA: row[6],
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
            query = f"""
                INSERT INTO {ENTITIES_TABLE} ({ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                                     {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA})
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            self.connection.execute(
                query,
                [
                    entity[ARTICLE_ID],
                    entity[SENTENCE_ID],
                    entity[TEXT],
                    entity[START_CHAR],
                    entity[END_CHAR],
                    entity.get(INFERENCE_MODEL),  # Optional fields
                    entity.get(INFERENCE_MODEL_METADATA),
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
                    f"""
                    INSERT INTO {ENTITIES_TABLE} ({ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                                         {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA})
                    SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT}, {START_CHAR}, {END_CHAR},
                           {INFERENCE_MODEL}, {INFERENCE_MODEL_METADATA}
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
            query = f"""
                SELECT a.{ARTICLE_ID}, a.{TITLE},
                       COUNT(DISTINCT s.{SENTENCE_ID}) AS sentence_count,
                       COUNT(e.{TEXT}) AS entity_count
                FROM {ARTICLES_TABLE} a
                LEFT JOIN {SENTENCES_TABLE} s ON a.{ARTICLE_ID} = s.{ARTICLE_ID}
                LEFT JOIN {ENTITIES_TABLE} e ON s.{ARTICLE_ID} = e.{ARTICLE_ID} AND s.{SENTENCE_ID} = e.{SENTENCE_ID}
                GROUP BY a.{ARTICLE_ID}, a.{TITLE}
                ORDER BY entity_count DESC
            """
            return self.connection.execute(query).fetchdf()
        except Exception as e:
            self.logger.error(f"Error getting entity statistics: {e}")
            raise
