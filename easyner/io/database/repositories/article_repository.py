import pandas as pd
from typing import Dict, List, Any, Union, Optional
import logging
import warnings

from easyner.io.database.utils.transaction import transactional
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    TITLE,
    ARTICLES_TABLE,
)

from .base import Repository
from ..connection import DatabaseConnection


class ArticleRepository(Repository):
    """
    Repository for managing article data in the database.

    Provides methods to retrieve, insert, and query article records.
    """

    def __init__(self, connection: DatabaseConnection):
        """
        Initialize ArticleRepository.

        Args:
            connection: Database connection object
        """
        self.logger = logging.getLogger(__name__)
        self.connection = connection

    def get_all(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all articles from the database.

        Args:
            as_df: Return as DataFrame if True, otherwise as list of dicts (default: True)

        Returns:
            DataFrame or list of article dictionaries
        """
        if as_df:
            return self.get_all_df()
        else:
            return self.get_all_dict_list()

    def get_all_df(self) -> pd.DataFrame:
        """
        Get all articles from the database as a DataFrame.

        Returns:
            DataFrame containing article data
        """
        try:
            query = f"SELECT {ARTICLE_ID}, {TITLE} FROM {ARTICLES_TABLE}"
            result = self.connection.execute(query)
            return result.fetchdf()
        except Exception as e:
            self.logger.error(f"Error retrieving articles as DataFrame: {e}")
            raise

    def get_all_dict_list(self) -> List[Dict[str, Any]]:
        """
        Get all articles from the database as a list of dictionaries.

        Returns:
            List of dictionaries containing article data
        """
        try:
            query = f"SELECT {ARTICLE_ID}, {TITLE} FROM {ARTICLES_TABLE}"
            rows = self.connection.execute(query).fetchall()
            return [{ARTICLE_ID: row[0], TITLE: row[1]} for row in rows]
        except Exception as e:
            self.logger.error(
                f"Error retrieving articles as dictionary list: {e}"
            )
            raise

    def get_by_id(self, article_id: int) -> Optional[Dict[str, Any]]:
        """
        Get an article by its ID.

        Args:
            article_id: ID of the article to retrieve

        Returns:
            Article data as a dictionary or None if not found
        """
        try:
            result = self.connection.execute(
                f"SELECT {ARTICLE_ID}, {TITLE} FROM {ARTICLES_TABLE} WHERE {ARTICLE_ID} = ?",
                [article_id],
            )
            row = result.fetchone()
            if row:
                return {ARTICLE_ID: row[0], TITLE: row[1]}
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving article by ID: {e}")
            raise

    def insert(self, article: Dict[str, Any]) -> None:
        """
        Insert an article into the database.

        Args:
            article: Article data as a dictionary with 'article_id' and 'title' keys
        """
        try:
            self.connection.execute(
                f"INSERT INTO {ARTICLES_TABLE} ({ARTICLE_ID}, {TITLE}) VALUES (?, ?)",
                [article[ARTICLE_ID], article[TITLE]],
            )
        except Exception as e:
            self.logger.error(f"Error inserting article: {e}")
            raise

    def _execute_insert_many(
        self, articles: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Core logic to insert multiple articles. Not transactional by itself.
        """
        if not isinstance(articles, (list, pd.DataFrame)):
            self.logger.error(
                "Invalid type for articles: Expected list of dictionaries or DataFrame."
            )
            raise TypeError(
                "articles must be a list of dictionaries or a pandas DataFrame."
            )

        if isinstance(articles, list):
            if not articles:  # Handle empty list
                return
            # Ensure all elements are dictionaries
            if not all(isinstance(a, dict) for a in articles):
                self.logger.error(
                    "Invalid format for articles list: Expected list of dictionaries."
                )
                raise ValueError(
                    "All items in articles list must be dictionaries."
                )
            df = pd.DataFrame(articles)
        else:  # isinstance(articles, pd.DataFrame)
            df = articles

        if df.empty:
            return

        # Ensure required columns are present
        required_cols = {ARTICLE_ID, TITLE}
        if not required_cols.issubset(df.columns):
            missing_cols = required_cols - set(df.columns)
            self.logger.error(
                f"DataFrame is missing required columns for articles: {missing_cols}"
            )
            raise ValueError(
                f"DataFrame for articles is missing columns: {missing_cols}"
            )

        # Use a unique view name to avoid conflicts if called multiple times in one transaction
        view_name = f"temp_articles_df_{id(df)}"
        try:
            # Select only the required columns for registration to avoid issues with extra columns
            self.connection.register(view_name, df[list(required_cols)])
            self.connection.execute(
                f"INSERT OR IGNORE INTO {ARTICLES_TABLE} ({ARTICLE_ID}, {TITLE}) SELECT {ARTICLE_ID}, {TITLE} FROM {view_name}"
            )
        finally:
            self.connection.unregister(view_name)  # Ensure cleanup

    @transactional
    def insert_many_transactional(
        self,
        articles: Union[List[Dict[str, Any]], pd.DataFrame],
    ) -> None:
        """
        Transactional wrapper of insert many.
        Use this for standalone batch insertions.

        Args:
            articles: List of article dictionaries or DataFrame containing article data

        Raises:
            Exception: If there is an error during the insertion process so the
            calling context (e.g., @transactional decorator  or an external
            transaction manager) can handle it, e.g., by rolling back.
        """
        # Exceptions from insert_many_within_transaction will propagate
        # to the @transactional decorator, which handles rollback and logging of the rollback.
        self.insert_many_non_transactional(articles)

    def insert_many_non_transactional(
        self, articles: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple articles.
        NOT transactional -> This should mostly not be used as standalone
        Recommended usage is for externally managed transactions

        Args:
            articles: List of article dictionaries or DataFrame containing article data
        """
        try:
            self._execute_insert_many(articles)
        except Exception as e:
            self.logger.error(
                f"Error batch inserting articles within an existing transaction: {e}"
            )

            raise
