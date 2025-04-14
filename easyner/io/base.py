from abc import ABC, abstractmethod
import os
from pathlib import Path
from typing import Optional


class IOHandler(ABC):
    """Abstract Base Class for file I/O operations."""

    DEFAULT_ENCODING = "utf-8"

    def __init__(self, encoding: Optional[str] = None):
        self.encoding = encoding or self.DEFAULT_ENCODING

    @abstractmethod
    def read(self, file_path: str, **kwargs):
        """Reads data from the specified file path."""
        pass

    @abstractmethod
    def write(self, data, file_path: str, **kwargs):
        """Writes data to the specified file path."""
        pass

    # --- Common Helper Methods (can be part of the base class) ---
    def ensure_dir_exists(self, file_path: str):
        """Ensures the directory for the given file path exists."""
        dir_path = os.path.dirname(file_path)
        if dir_path:  # Only try to create if there's actually a directory part
            os.makedirs(dir_path, exist_ok=True)

    def check_file_exists(self, file_path: str):
        """Checks if a file exists, raising FileNotFoundError if not."""
        if not Path(file_path).is_file():
            raise FileNotFoundError(f"Input file not found: {file_path}")
