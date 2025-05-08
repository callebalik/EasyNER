from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional, Union


class BaseConverter(ABC):
    """Abstract base class for file converters."""

    def __init__(
        self,
        source_dir: Union[str, Path],
        target_dir: Optional[Union[str, Path]] = None,
    ):
        """Initialize the converter with source and target directories.

        Args:
            source_dir: Directory containing files to be converted
            target_dir: Directory where converted files will be stored
            (if applicable)

        """
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir) if target_dir else None

    @abstractmethod
    def list_convertible_files(self) -> list[Path]:
        """List eligible files for conversion by this converter
        in the source directory.

        Returns:
            List of file paths that can be converted

        """
        pass

    @abstractmethod
    def list_converted_files(self) -> list[Path]:
        """List all files that have already been converted

        Returns:
            List of files already converted

        """
        pass

    def list_unconverted_files(self) -> list[Path]:
        """List unconverted files in source directory"""
        try:
            convertible_files_set = {
                p.resolve() for p in self.list_convertible_files()
            }
            converted_files_set = {
                p.resolve() for p in self.list_converted_files()
            }

            unconverted_paths = list(
                convertible_files_set - converted_files_set,
            )
            return [Path(p) for p in unconverted_paths]
        except Exception as e:
            # Log error or handle as appropriate for your application
            print(f"Error listing unconverted files: {e}")
            return []

    @abstractmethod
    def convert(self, **kwargs) -> dict[str, Any]:
        """Convert the source files according to converter implementation

        Args:
            **kwargs: Additional arguments specific to the converter
            implementation

        Returns:
            Dictionary containing information about the conversion process

        """
        pass
