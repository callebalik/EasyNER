import pandas as pd
from typing import Dict, List, Any, Union, Optional
import logging
import warnings

from easyner.io.database.utils.transaction import transactional

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
            result = self.connection.execute(
                "SELECT article_id, title FROM articles"
            )
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
            rows = self.connection.execute(
                "SELECT article_id, title FROM articles"
            ).fetchall()
            return [{"article_id": row[0], "title": row[1]} for row in rows]
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
                "SELECT article_id, title FROM articles WHERE article_id = ?",
                [article_id],
            )
            row = result.fetchone()
            if row:
                return {"article_id": row[0], "title": row[1]}
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
                "INSERT INTO articles (article_id, title) VALUES (?, ?)",
                [article["article_id"], article["title"]],
            )
        except Exception as e:
            self.logger.error(f"Error inserting article: {e}")
            raise

    @transactional
    def insert_many(
        self, articles: Union[List[Dict[str, Any]], pd.DataFrame]
    ) -> None:
        """
        Insert multiple articles into the database.

        Args:
            articles: List of article dictionaries or DataFrame containing article data
        """
        try:
            # If given a DataFrame, register it as a view
            if isinstance(articles, pd.DataFrame):
                self.connection.register("articles_df", articles)
                self.connection.execute(
                    "INSERT INTO articles SELECT * FROM articles_df"
                )
            else:
                # For list of dictionaries, process each one
                for article in articles:
                    self.insert(article)
        except Exception as e:
            self.logger.error(f"Error batch inserting articles: {e}")
            raise
