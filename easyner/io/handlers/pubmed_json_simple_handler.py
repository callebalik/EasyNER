from typing import Any, Dict, Optional  # Removed List
from easyner.io.handlers.json_handler import JsonHandler
import logging
import json
import os
from jsonschema import validate, ValidationError

# Initialize logger
logger = logging.getLogger(__name__)

# Define paths relative to this file for clarity and line length
_HANDLER_FILE_PATH = os.path.abspath(__file__)
_HANDLERS_DIR = os.path.dirname(_HANDLER_FILE_PATH)
_IO_DIR = os.path.dirname(_HANDLERS_DIR)  # This should be easyner/io


class PubMedJsonHandlerSimple(JsonHandler):
    """
    Specialized JsonHandler for PubMed data.
    """

    SCHEMA_DIR = os.path.join(_IO_DIR, "schemas")  # Path: easyner/io/schemas
    SCHEMA_FILE = "pubmed.schema.json"
    EXTENSION = "json"

    def __init__(
        self, encoding: str = "utf-8", validate_schema: bool = False
    ) -> None:
        super().__init__(encoding=encoding)
        self.validate_schema = validate_schema
        self.schema: Optional[Dict[str, Any]] = None

        if self.validate_schema:
            schema_path = os.path.join(self.SCHEMA_DIR, self.SCHEMA_FILE)
            try:
                if not os.path.exists(schema_path):
                    logger.warning(
                        f"Schema file not found at {schema_path}. "
                        "Schema validation disabled."
                    )
                    self.validate_schema = False
                else:
                    with open(schema_path, "r", encoding=encoding) as f:
                        self.schema = json.load(f)
            except Exception as e:
                logger.warning(
                    f"Failed to load PubMed schema from {schema_path}: {e}. "
                    "Schema validation disabled."
                )
                self.validate_schema = False

    def read(
        self, file_path: str, timeout: int = 180, **kwargs: Any
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = super().read(
            file_path, timeout=timeout, **kwargs
        )

        if self.validate_schema and self.schema is not None:
            try:
                validate(instance=data, schema=self.schema)
                logger.debug(
                    f"Successfully validated {os.path.basename(file_path)} "
                    "against PubMed schema"
                )
            except ValidationError as e:
                logger.warning(
                    f"Schema validation failed for "
                    f"{os.path.basename(file_path)}: {e}"
                )
        return data
