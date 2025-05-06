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

    def _execute_insert_many(
        self, entities: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Core logic to insert multiple entities. Not transactional by itself.
        """
        if not isinstance(entities, (list, pd.DataFrame)):
            self.logger.error(
                "Invalid type for entities: Expected list of dictionaries or DataFrame."
            )
            raise TypeError(
                "entities must be a list of dictionaries or a pandas DataFrame."
            )

        if isinstance(entities, list):
            if not entities:  # Handle empty list
                return
            if not all(isinstance(e, dict) for e in entities):
                self.logger.error(
                    "Invalid format for entities list: Expected list of dictionaries."
                )
                raise ValueError(
                    "All items in entities list must be dictionaries."
                )
            df = pd.DataFrame(entities)
        else:  # isinstance(entities, pd.DataFrame)
            df = entities

        if df.empty:
            return

        required_cols = {ARTICLE_ID, SENTENCE_ID, TEXT, START_CHAR, END_CHAR}
        # Optional columns that might be present
        optional_cols = {INFERENCE_MODEL, INFERENCE_MODEL_METADATA}

        if not required_cols.issubset(df.columns):
            missing_cols = required_cols - set(df.columns)
            self.logger.error(
                f"DataFrame is missing required columns for entities: {missing_cols}"
            )
            raise ValueError(
                f"DataFrame for entities is missing columns: {missing_cols}"
            )

        # Determine which optional columns are actually in the DataFrame
        present_optional_cols = list(optional_cols.intersection(df.columns))
        cols_to_insert = list(required_cols) + present_optional_cols

        # Construct column names string for SQL query
        sql_column_names = ", ".join(cols_to_insert)

        # Select only the columns that will be inserted
        df_to_register = df[
            cols_to_insert
        ].copy()  # Use .copy() to avoid SettingWithCopyWarning

        # Handle potential NaN values in optional columns if they are numeric,
        # or ensure they are suitable for DB insertion (e.g. None for text)
        for col in present_optional_cols:
            # If a column is all NaN, DuckDB might infer it as float, then fail on string insert.
            # Convert to object to allow None/NULL.
            if df_to_register[col].isnull().all():
                df_to_register[col] = None
            elif (
                pd.api.types.is_numeric_dtype(df_to_register[col].dtype)
                and df_to_register[col].isnull().any()
            ):
                # For numeric columns with NaNs that are not all NaNs,
                # convert to a type that supports NaNs suitable for DuckDB (e.g., float or allow DuckDB to handle with None)
                # This step might be nuanced based on exact DB schema and desired NaN representation (e.g., NULL vs. a specific number)
                # For simplicity, if it's numeric and has NaNs, we ensure they are treated as None for SQL NULL.
                df_to_register[col] = df_to_register[col].where(
                    pd.notnull(df_to_register[col]), None
                )

        view_name = f"temp_entities_df_{id(df_to_register)}"
        try:
            self.connection.register(view_name, df_to_register)
            self.connection.execute(
                f"INSERT INTO {ENTITIES_TABLE} ({sql_column_names}) SELECT {sql_column_names} FROM {view_name}"
            )
        finally:
            self.connection.unregister(view_name)

    @transactional
    def insert_many(
        self, entities: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple entities into the database. This method is transactional.
        Use this for standalone batch insertions.

        Args:
            entities: List of entity dictionaries or DataFrame containing entity data
        """
        try:
            self._execute_insert_many(entities)
        except Exception as e:
            self.logger.error(f"Error batch inserting entities: {e}")
            # The @transactional decorator will handle rollback
            raise

    def insert_many_within_transaction(
        self, entities: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple entities as part of an existing, externally managed transaction.
        This method is NOT transactional by itself.

        Args:
            entities: List of entity dictionaries or DataFrame containing entity data
        """
        try:
            self._execute_insert_many(entities)
        except Exception as e:
            self.logger.error(
                f"Error batch inserting entities within an existing transaction: {e}"
            )
            # Let the external transaction handler decide on rollback
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
