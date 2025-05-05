import json
import os
from pathlib import Path
from typing import List, Dict, Any, Union, Optional

from easyner.io.converters.abstract_converter import AbstractConverter
from easyner.io.database.db_utils import (
    initialize_db,
    create_tables,
    insert_data,
    create_indices,
    get_table_count,
)


class JsonToDuckConverter(AbstractConverter):
    """Converter to transform JSON data into DuckDB database"""

    def __init__(
        self,
        source_dir: Union[str, Path],
        target_dir: Union[str, Path],
        connection=None,
        db_file: Optional[str] = None,
        file_pattern: str = "*.json",
    ):
        """
        Initialize the JSON to DuckDB converter

        Args:
            source_dir: Directory containing JSON files to convert
            target_dir: Directory where the DuckDB database will be stored
            connection: An existing DuckDB connection (optional)
            db_file: Custom database filename (optional, default is 'easyner.db')
            file_pattern: Pattern to match JSON files (default: "*.json")
        """
        super().__init__(source_dir, target_dir)
        self.file_pattern = file_pattern

        # Set up db_file path - if not provided, create one in target_dir
        if db_file is None:
            self.db_file = os.path.join(str(target_dir), "easyner.db")
        else:
            self.db_file = db_file

        self.connection = connection
        self._converted_files = []

    def list_convertible_files(self) -> List[Path]:
        """List all JSON files in the source directory"""
        return list(self.source_dir.glob(self.file_pattern))

    def list_converted_files(self) -> List[Path]:
        """List all files that have already been converted"""
        return self._converted_files

    def _process_json_file(
        self, file_path: Path
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process a single JSON file and extract structured data

        Args:
            file_path: Path to the JSON file

        Returns:
            Dictionary containing articles, sentences and entities data
        """
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        articles_data = []
        sentences_data = []
        entities_data = []

        # Process data in a single pass
        for article_id_str, article_content in data.items():
            # Convert article_id from string to integer
            article_id = int(article_id_str)

            # Articles table
            articles_data.append(
                {
                    "article_id": article_id,
                    "title": article_content.get("title", ""),
                }
            )

            sentences = article_content.get("sentences", [])

            # Process sentences and entities
            for sentence_idx, sentence in enumerate(sentences):
                sentences_data.append(
                    {
                        "article_id": article_id,
                        "sentence_id": sentence_idx,
                        "text": sentence.get("text", ""),
                    }
                )

                # Get entities once
                entities = sentence.get("entities", [])
                entity_spans = sentence.get("entity_spans", [])

                # Extend entities_data with all valid entities at once
                entities_data.extend(
                    {
                        "article_id": article_id,
                        "sentence_id": sentence_idx,
                        "entity": entity,
                        "start_pos": span[0] if span else None,
                        "end_pos": span[1] if span else None,
                        "inference_model": None,
                        "inference_model_metadata": None,
                    }
                    for entity, span in zip(entities, entity_spans)
                    if entity  # Skip empty entities
                )

        return {
            "articles": articles_data,
            "sentences": sentences_data,
            "entities": entities_data,
        }

    def convert(self, **kwargs) -> Dict[str, Any]:
        """
        Convert JSON files to DuckDB database

        Args:
            **kwargs: Additional arguments
                use_memory_db (bool): Use an in-memory database instead of a file
                    (default: False)

        Returns:
            Dictionary containing statistics about the conversion
        """
        # Check if we should use an in-memory database
        use_memory_db = kwargs.get("use_memory_db", False)

        # Initialize database if not provided
        if not self.connection:
            if use_memory_db:
                db_path = ":memory:"
            else:
                # Ensure the parent directory exists
                os.makedirs(os.path.dirname(self.db_file), exist_ok=True)
                db_path = self.db_file

            self.connection = initialize_db(database_path=db_path)

        # Create database tables
        create_tables(self.connection)

        # Process all JSON files
        convertible_files = self.list_convertible_files()

        total_articles = 0
        total_sentences = 0
        total_entities = 0

        # Process each file
        for file_path in convertible_files:
            data = self._process_json_file(file_path)

            # Insert data into database
            insert_data(
                self.connection,
                data["articles"],
                data["sentences"],
                data["entities"],
            )

            # Track processed files
            self._converted_files.append(file_path)

            # Update counts
            total_articles += len(data["articles"])
            total_sentences += len(data["sentences"])
            total_entities += len(data["entities"])

        # Create indices for better performance
        create_indices(self.connection)

        # Get final counts
        article_count = get_table_count(self.connection, "articles")
        sentence_count = get_table_count(self.connection, "sentences")
        entity_count = get_table_count(self.connection, "entities")

        # Return statistics
        result = {
            "files_processed": len(self._converted_files),
            "article_count": article_count,
            "sentence_count": sentence_count,
            "entity_count": entity_count,
            "processed_files": [str(f) for f in self._converted_files],
            "database_path": self.db_file if not use_memory_db else ":memory:",
        }

        return result
