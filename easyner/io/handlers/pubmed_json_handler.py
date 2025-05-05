from typing import Dict, List, Tuple, Optional, Union
import pandas as pd
from easyner.io.handlers.json_handler import JsonHandler
import logging
import json
import os
from jsonschema import validate, ValidationError

# Initialize logger
logger = logging.getLogger(__name__)


class PubMedJsonHandler(JsonHandler):
    """
    Specialized JsonHandler for PubMed data with methods to extract articles, sentences, and entities.
    """

    # Directory containing the schema
    SCHEMA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schemas"
    )
    SCHEMA_FILE = "pubmed.schema.json"
    EXTENSION = "json"

    def __init__(self, encoding="utf-8", validate_schema=True):
        """
        Initialize the PubMed JSON handler with encoding

        Args:
            encoding: Character encoding for file operations
            validate_schema: Whether to validate data against the PubMed schema
        """
        super().__init__(encoding=encoding)
        self.validate_schema = validate_schema
        self.schema = None

        # Load the schema if validation is enabled
        if validate_schema:
            try:
                schema_path = os.path.join(self.SCHEMA_DIR, self.SCHEMA_FILE)
                with open(schema_path, "r", encoding=encoding) as f:
                    self.schema = json.load(f)
            except Exception as e:
                logger.warning(
                    f"Failed to load PubMed schema: {str(e)}. Schema validation disabled."
                )
                self.validate_schema = False

    def read(self, file_path: str, timeout=180, **kwargs):
        """
        Read and validate PubMed JSON data from a file

        Args:
            file_path: Path to the JSON file
            timeout: Timeout in seconds for reading operation
            **kwargs: Additional arguments for reading

        Returns:
            Parsed JSON data
        """
        data = super().read(file_path, timeout, **kwargs)

        # Validate against schema if enabled
        if self.validate_schema and self.schema is not None:
            try:
                validate(instance=data, schema=self.schema)
                logger.debug(
                    f"Successfully validated {file_path} against PubMed schema"
                )
            except ValidationError as e:
                logger.warning(
                    f"Schema validation failed for {file_path}: {str(e)}"
                )

        return data

    def _process_article(self, article_id: int, article_data: Dict) -> Dict:
        """Extract article information from article data with optimized memory usage"""
        # Pre-allocate dictionary with common fields
        article = {
            "article_id": article_id,  # Already converted to integer
            "title": article_data.get("title", ""),
            "abstract": article_data.get("abstract", ""),
        }

        # Add metadata selectively to avoid dictionary resizing
        metadata = article_data.get("metadata", {})
        if metadata:
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)):
                    article[key] = value

        return article

    def _process_sentence(
        self, article_id: int, sent_idx: int, sentence_data: Dict
    ) -> Dict:
        """Extract sentence information from sentence data"""
        # Use integer position for proper ordering
        sentence = {
            "sentence_id": sent_idx,  # Use integer as sentence_id (unique within article)
            "article_id": article_id,  # Already an integer
            "position": sent_idx,  # Order within article
            "text": sentence_data.get("text", ""),
        }

        # Add tokens if available
        tokens = sentence_data.get("tokens", [])
        if tokens:
            sentence["tokens"] = tokens

        return sentence

    def _process_entities(
        self, article_id: int, sent_idx: int, sentence_data: Dict
    ) -> List[Dict]:
        """Extract entity information from sentence data"""
        entity_list = sentence_data.get("entities", [])
        if not entity_list:
            return []

        entity_spans = sentence_data.get("entity_spans", [])

        # Skip entirely if there are entities but no spans
        if not entity_spans:
            logger.warning(
                f"Skipping entities in article {article_id}, sentence {sent_idx}: "
                f"Found {len(entity_list)} entities but no entity spans"
            )
            return []

        # Log warning for mismatched lengths
        if len(entity_list) != len(entity_spans):
            logger.warning(
                f"Mismatched entity data in article {article_id}, sentence {sent_idx}: "
                f"Found {len(entity_list)} entities but only {len(entity_spans)} spans"
            )
            # Skip processing when there's a mismatch in counts
            if len(entity_list) > len(entity_spans):
                return []

        # Get these just once per sentence
        entity_names = sentence_data.get("names", [])

        # Process entities
        result = []

        # Only process when entity list and spans match in length
        for ent_idx in range(len(entity_list)):
            entity_text = entity_list[ent_idx]

            # Skip empty entities and log warning
            if not entity_text:
                logger.warning(
                    f"Empty entity text at position {ent_idx} in article {article_id}"
                )
                continue

            # Get the span for this entity
            span = entity_spans[ent_idx]

            # Create entity object
            result.append(
                {
                    "entity_id": ent_idx,
                    "sentence_id": sent_idx,  # Integer sentence ID
                    "article_id": article_id,
                    "text": entity_text,
                    "start_char": int(span[0]) if len(span) > 0 else None,
                    "end_char": int(span[1]) if len(span) > 1 else None,
                    "entity_name": (
                        entity_names[ent_idx]
                        if ent_idx < len(entity_names)
                        else None
                    ),
                }
            )

        return result

    def extract_all_dataframes(
        self, data: Dict
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Extract articles, sentences, and entities from PubMed JSON in a single pass

        Args:
            data: Loaded JSON data

        Returns:
            Tuple of (articles_df, sentences_df, entities_df)
        """
        articles = []
        sentences = []
        entities = []

        # Process all data in a single pass
        for article_id_str, article_data in data.items():
            # Convert article_id to integer once at the beginning
            try:
                article_id = int(article_id_str)
            except (ValueError, TypeError):
                logger.warning(
                    f"Could not convert article_id '{article_id_str}' to integer, using as is"
                )
                article_id = article_id_str

            # Process article
            article = self._process_article(article_id, article_data)
            articles.append(article)

            # Process sentences and entities
            for sent_idx, sentence_data in enumerate(
                article_data.get("sentences", [])
            ):
                # Process sentence
                sentence = self._process_sentence(
                    article_id, sent_idx, sentence_data
                )
                sentences.append(sentence)

                # Process entities
                sent_entities = self._process_entities(
                    article_id, sent_idx, sentence_data
                )
                entities.extend(sent_entities)

        # Create DataFrames
        articles_df = pd.DataFrame(articles) if articles else pd.DataFrame()
        sentences_df = pd.DataFrame(sentences) if sentences else pd.DataFrame()
        entities_df = pd.DataFrame(entities) if entities else pd.DataFrame()

        return (articles_df, sentences_df, entities_df)

    def extract_articles_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract article information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with article information
        """
        articles_df, _, _ = self.extract_all_dataframes(data)
        return articles_df

    def extract_sentences_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract sentence information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with sentence information
        """
        _, sentences_df, _ = self.extract_all_dataframes(data)
        return sentences_df

    def extract_entities_dataframe(self, data: Dict) -> pd.DataFrame:
        """
        Extract entity information from PubMed JSON into a dataframe

        Args:
            data: Loaded JSON data

        Returns:
            DataFrame with entity information
        """
        _, _, entities_df = self.extract_all_dataframes(data)
        return entities_df

    def get_sentence_count(self, data: Dict) -> int:
        """
        Get the total number of sentences across all articles

        Args:
            data: Loaded JSON data

        Returns:
            Total number of sentences
        """
        count = 0
        for article_data in data.values():
            count += len(article_data.get("sentences", []))
        return count

    def get_entity_count(self, data: Dict) -> int:
        """
        Get the total number of entities across all articles and sentences

        Args:
            data: Loaded JSON data

        Returns:
            Total number of entities
        """
        count = 0
        for article_data in data.values():
            for sentence in article_data.get("sentences", []):
                count += len(sentence.get("entities", []))
        return count

    def filter_articles_by_metadata(
        self, data: Dict, filter_criteria: Dict
    ) -> Dict:
        """
        Filter articles by metadata fields

        Args:
            data: Loaded JSON data
            filter_criteria: Dictionary of metadata field names and values to match

        Returns:
            Filtered dictionary of articles
        """
        result = {}

        for article_id, article_data in data.items():
            metadata = article_data.get("metadata", {})
            if all(
                metadata.get(key) == value
                for key, value in filter_criteria.items()
            ):
                result[article_id] = article_data

        return result

    def export_to_csv(
        self, data: Dict, output_dir: str, prefix: str = "pubmed"
    ) -> Dict:
        """
        Export PubMed data to CSV files (articles, sentences, entities)

        Args:
            data: Loaded JSON data
            output_dir: Directory to write CSV files
            prefix: Prefix for the CSV filenames

        Returns:
            Dictionary with paths to the output files
        """
        os.makedirs(output_dir, exist_ok=True)

        articles_df, sentences_df, entities_df = self.extract_all_dataframes(
            data
        )

        articles_path = os.path.join(output_dir, f"{prefix}_articles.csv")
        sentences_path = os.path.join(output_dir, f"{prefix}_sentences.csv")
        entities_path = os.path.join(output_dir, f"{prefix}_entities.csv")

        articles_df.to_csv(articles_path, index=False)
        sentences_df.to_csv(sentences_path, index=False)
        entities_df.to_csv(entities_path, index=False)

        return {
            "articles": articles_path,
            "sentences": sentences_path,
            "entities": entities_path,
        }
