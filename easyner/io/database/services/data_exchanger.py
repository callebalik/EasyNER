"""Service for importing and exporting data across multiple repositories."""

import logging
from typing import Any, Dict, List, Optional

from easyner.io.database.connection import DatabaseConnection
from easyner.io.database.repositories import (
    ArticleRepository,
    EntityRepository,
    SentenceRepository,
)
from easyner.io.database.utils.transaction import transactional


class DataExchanger:
    """Orchestrates data exchange between multiple repositories.
    
    This service handles operations that span multiple repositories,
    such as importing complete articles with their sentences and entities.
    It maintains referential integrity and provides hierarchical duplicate handling.
    """
    
    def __init__(
        self, 
        connection: DatabaseConnection,
        article_repo: Optional[ArticleRepository] = None,
        sentence_repo: Optional[SentenceRepository] = None,
        entity_repo: Optional[EntityRepository] = None
    ) -> None:
        """Initialize with database connection and repositories.
        
        Args:
            connection: Database connection to use
            article_repo: ArticleRepository instance (created if None)
            sentence_repo: SentenceRepository instance (created if None)
            entity_repo: EntityRepository instance (created if None)
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.connection = connection
        
        # Initialize repositories if not provided
        self.article_repository = article_repo or ArticleRepository(connection)
        self.sentence_repository = sentence_repo or SentenceRepository(connection)
        self.entity_repository = entity_repo or EntityRepository(connection)
    
    @transactional
    def import_article_with_sentences_and_entities(
        self,
        article_data: Dict[str, Any],
        sentences_data: List[Dict[str, Any]],
        entities_data: List[Dict[str, Any]],
        log_duplicates: bool = True,
    ) -> None:
        """Import complete article data with hierarchical duplicate handling.
        
        This method:
        1. Imports the article, tracking any duplicates
        2. Imports sentences, excluding those belonging to duplicate articles
        3. Imports entities, excluding those belonging to duplicate sentences or articles
        4. Maintains proper referential integrity throughout the process
        
        Args:
            article_data: Article data dictionary 
            sentences_data: List of sentence dictionaries
            entities_data: List of entity dictionaries
            log_duplicates: Flag to control duplicate logging
        """
        self.logger.info(f"Starting hierarchical import for article {article_data.get('article_id')}")
        
        try:
            # Step 1: Process article, creating temp_article_duplicates for tracking
            self._import_article_with_duplicate_tracking(
                article_data,
                "temp_article_duplicates"
            )
            
            # Step 2: Process sentences using article duplicates info
            self._import_sentences_with_parent_tracking(
                sentences_data,
                "temp_article_duplicates",
                "temp_sentence_duplicates"
            )
            
            # Step 3: Process entities using sentence duplicates info
            self._import_entities_with_parent_tracking(
                entities_data,
                "temp_sentence_duplicates"
            )
            
            self.logger.info("Successfully completed hierarchical import")
            
        finally:
            # Step 4: Cleanup temp tables
            self.connection.execute("DROP TABLE IF EXISTS temp_article_duplicates")
            self.connection.execute("DROP TABLE IF EXISTS temp_sentence_duplicates")
    
    def _import_article_with_duplicate_tracking(
        self, 
        article_data: Dict[str, Any],
        duplicate_tracking_table: str
    ) -> None:
        """Import article and track duplicates in specified table."""
        # Implementation for article import with duplicate tracking
        # ...
        pass
    
    def _import_sentences_with_parent_tracking(
        self,
        sentences_data: List[Dict[str, Any]],
        parent_duplicates_table: str,
        output_duplicates_table: str
    ) -> None:
        """Import sentences with awareness of parent article duplicates."""
        # Implementation for sentence import with parent tracking
        # ...
        pass
    
    def _import_entities_with_parent_tracking(
        self,
        entities_data: List[Dict[str, Any]],
        parent_duplicates_table: str
    ) -> None:
        """Import entities with awareness of parent sentence duplicates."""
        # Implementation for entity import with parent tracking
        # ...
        pass