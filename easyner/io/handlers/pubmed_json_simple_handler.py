from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Union
import pandas as pd
from easyner.io.handlers.json_handler import JsonHandler
import logging
import json
import os
from jsonschema import validate, ValidationError
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    TITLE,
    TEXT,
    SENTENCE_ID,
    START_CHAR,
    END_CHAR,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
)

# Initialize logger
logger = logging.getLogger(__name__)


class PubMedJsonHandlerSimple(JsonHandler):
    """
    Specialized JsonHandler for PubMed data with methods to extract articles, sentences, and entities.
    """

    # Directory containing the schema
    SCHEMA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "schemas"
    )
    SCHEMA_FILE = "pubmed.schema.json"
    EXTENSION = "json"

    def __init__(self, encoding="utf-8", validate_schema=False):
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

    from typing import Any, Dict, List, Tuple, Optional, Union


import pandas as pd
from easyner.io.handlers.json_handler import JsonHandler
import logging
import json
import os
from jsonschema import validate, ValidationError
from easyner.io.database.utils.column_names import (
    ARTICLE_ID,
    TITLE,
    TEXT,
    SENTENCE_ID,
    START_CHAR,
    END_CHAR,
    INFERENCE_MODEL,
    INFERENCE_MODEL_METADATA,
)

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
        # Read the JSON file
        data = self.read(file_path)

        articles_data = []
        sentences_data = []
        entities_data = []

        # Resolved file path to use as foreign key to conversion_log
        resolved_file_path = str(file_path.resolve())

        # Process data in a single pass
        for article_id_str, article_content in data.items():
            # Convert article_id from string to integer
            article_id = int(article_id_str)

            # Articles table - include source_file reference
            articles_data.append(
                {
                    ARTICLE_ID: article_id,
                    TITLE: article_content.get("title", ""),
                    "source_file": resolved_file_path,  # Add source file reference
                }
            )

            sentences = article_content.get("sentences", [])

            # Process sentences and entities
            for sentence_idx, sentence in enumerate(sentences):
                sentences_data.append(
                    {
                        ARTICLE_ID: article_id,
                        SENTENCE_ID: sentence_idx,
                        TEXT: sentence.get("text", ""),
                    }
                )

                # Get entities once
                entities = sentence.get("entities", [])
                entity_spans = sentence.get("entity_spans", [])

                # Extend entities_data with all valid entities at once
                entities_data.extend(
                    {
                        ARTICLE_ID: article_id,
                        SENTENCE_ID: sentence_idx,
                        TEXT: entity,
                        START_CHAR: span[0] if span else None,
                        END_CHAR: span[1] if span else None,
                        INFERENCE_MODEL: None,
                        INFERENCE_MODEL_METADATA: None,
                    }
                    for entity, span in zip(entities, entity_spans)
                    if entity  # Skip empty entities
                )

        return {
            "articles": articles_data,
            "sentences": sentences_data,
            "entities": entities_data,
        }
