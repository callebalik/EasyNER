import pandas as pd
from typing import Dict, List, Any, Union, Optional
import logging
import warnings

from easyner.io.database.utils.transaction import transactional
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    SENTENCE_ID,
    TEXT,
    SENTENCES_TABLE,
)

from .base import Repository
from ..connection import DatabaseConnection


class SentenceRepository(Repository):
    """
    Repository for managing sentence data in the database.

    Provides methods to retrieve, insert, and query sentence records.
    """

    def __init__(self, connection: DatabaseConnection):
        """
        Initialize SentenceRepository.

        Args:
            connection: Database connection object
        """
        self.logger = logging.getLogger(__name__)
        self.connection = connection

    def get_all(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all sentences from the database.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of sentence dictionaries
        """
        if as_df:
            return self.get_all_df()
        else:
            return self.get_all_dict_list()

    def get_all_df(self) -> pd.DataFrame:
        """
        Get all sentences from the database as a DataFrame.

        Returns:
            DataFrame containing sentence data
        """
        try:
            result = self.connection.execute(
                f"SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT} FROM {SENTENCES_TABLE}"
            )
            return result.fetchdf()
        except Exception as e:
            self.logger.error(f"Error retrieving sentences as DataFrame: {e}")
            raise

    def get_all_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all sentences from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing sentence data
        """
        try:
            rows = self.connection.execute(
                f"SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT} FROM {SENTENCES_TABLE}"
            ).fetchall()
            return [
                {ARTICLE_ID: row[0], SENTENCE_ID: row[1], TEXT: row[2]}
                for row in rows
            ]
        except Exception as e:
            self.logger.error(
                f"Error retrieving sentences as dictionary list: {e}"
            )
            raise

    def get_by_article_id(
        self, article_id: int, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get sentences for a specific article.

        Args:
            article_id: Article ID to filter by
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of sentence dictionaries for the specified article
        """
        try:
            result = self.connection.execute(
                f"SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT} FROM {SENTENCES_TABLE} WHERE {ARTICLE_ID} = ?",
                [article_id],
            )

            if as_df:
                return result.fetchdf()
            else:
                rows = result.fetchall()
                return [
                    {
                        ARTICLE_ID: row[0],
                        SENTENCE_ID: row[1],
                        TEXT: row[2],
                    }
                    for row in rows
                ]
        except Exception as e:
            self.logger.error(f"Error retrieving sentences by article ID: {e}")
            raise

    def get_by_id(
        self, article_id: int, sentence_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific sentence by its article ID and sentence ID.

        Args:
            article_id: Article ID
            sentence_id: Sentence ID

        Returns:
            Sentence data as a dictionary or None if not found
        """
        try:
            result = self.connection.execute(
                f"SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT} FROM {SENTENCES_TABLE} WHERE {ARTICLE_ID} = ? AND {SENTENCE_ID} = ?",
                [article_id, sentence_id],
            )
            row = result.fetchone()
            if row:
                return {
                    ARTICLE_ID: row[0],
                    SENTENCE_ID: row[1],
                    TEXT: row[2],
                }
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving sentence by IDs: {e}")
            raise

    def insert(self, sentence: Dict[str, Any]) -> None:
        """
        Insert a sentence into the database.

        Args:
            sentence: Sentence data as a dictionary with 'article_id', 'sentence_id', and 'text' keys
        """
        try:
            self.connection.execute(
                f"INSERT INTO {SENTENCES_TABLE} ({ARTICLE_ID}, {SENTENCE_ID}, {TEXT}) VALUES (?, ?, ?)",
                [
                    sentence[ARTICLE_ID],
                    sentence[SENTENCE_ID],
                    sentence[TEXT],
                ],
            )
        except Exception as e:
            self.logger.error(f"Error inserting sentence: {e}")
            raise

    def _execute_insert_many(
        self, sentences: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Core logic to insert multiple sentences. Not transactional by itself.
        """
        if not isinstance(sentences, (list, pd.DataFrame)):
            self.logger.error(
                "Invalid type for sentences: Expected list of dictionaries or DataFrame."
            )
            raise TypeError(
                "sentences must be a list of dictionaries or a pandas DataFrame."
            )

        if isinstance(sentences, list):
            if not sentences:  # Handle empty list
                return
            # Ensure all elements are dictionaries
            if not all(isinstance(s, dict) for s in sentences):
                self.logger.error(
                    "Invalid format for sentences list: Expected list of dictionaries."
                )
                raise ValueError(
                    "All items in sentences list must be dictionaries."
                )
            df = pd.DataFrame(sentences)
        else:  # isinstance(sentences, pd.DataFrame)
            df = sentences

        if df.empty:
            return

        # Ensure required columns are present
        required_cols = {ARTICLE_ID, SENTENCE_ID, TEXT}
        if not required_cols.issubset(df.columns):
            missing_cols = required_cols - set(df.columns)
            self.logger.error(
                f"DataFrame is missing required columns for sentences: {missing_cols}"
            )
            raise ValueError(
                f"DataFrame for sentences is missing columns: {missing_cols}"
            )

        # Use a unique view name to avoid conflicts if called multiple times in one transaction
        view_name = f"temp_sentences_df_{id(df)}"
        try:
            # Select only the required columns for registration
            self.connection.register(view_name, df[list(required_cols)])
            self.connection.execute(
                f"INSERT INTO {SENTENCES_TABLE} ({ARTICLE_ID}, {SENTENCE_ID}, {TEXT}) SELECT {ARTICLE_ID}, {SENTENCE_ID}, {TEXT} FROM {view_name}"
            )
        finally:
            self.connection.unregister(view_name)  # Ensure cleanup

    @transactional
    def insert_many(
        self, sentences: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple sentences into the database. This method is transactional.
        Use this for standalone batch insertions.

        Args:
            sentences: List of sentence dictionaries or DataFrame containing sentence data
        """
        try:
            self._execute_insert_many(sentences)
        except Exception as e:
            self.logger.error(f"Error batch inserting sentences: {e}")
            # The @transactional decorator will handle rollback
            raise

    def insert_many_within_transaction(
        self, sentences: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple sentences as part of an existing, externally managed transaction.
        This method is NOT transactional by itself.

        Args:
            sentences: List of sentence dictionaries or DataFrame containing sentence data
        """
        try:
            self._execute_insert_many(sentences)
        except Exception as e:
            self.logger.error(
                f"Error batch inserting sentences within an existing transaction: {e}"
            )
            # Let the external transaction handler decide on rollback
            raise

    def get_sentence_count_by_article(self) -> pd.DataFrame:
        """
        Get the number of sentences per article.

        Returns:
            DataFrame with article_id and sentence count
        """
        try:
            query = f"""
                SELECT {ARTICLE_ID}, COUNT(*) as sentence_count
                FROM {SENTENCES_TABLE}
                GROUP BY {ARTICLE_ID}
                ORDER BY {ARTICLE_ID}
            """
            return self.connection.execute(query).fetchdf()
        except Exception as e:
            self.logger.error(f"Error getting sentence counts by article: {e}")
            raise
