from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Dict, Any, Union


class AbstractConverter(ABC):
    """Abstract base class for file converters"""

    def __init__(
        self,
        source_dir: Union[str, Path],
        target_dir: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the converter with source and target directories

        Args:
            source_dir: Directory containing files to be converted
            target_dir: Directory where converted files will be stored (if applicable)
        """
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir) if target_dir else None

    @abstractmethod
    def list_convertible_files(self) -> List[Path]:
        """
        List all files in the source directory that can be converted by this converter

        Returns:
            List of file paths that can be converted
        """
        pass

    def list_converted_files(self) -> List[Path]:
        """
        List all files that have already been converted

        Returns:
            List of files already converted, empty list if tracking not supported
        """
        return []

    @abstractmethod
    def convert(self, **kwargs) -> Dict[str, Any]:
        """
        Convert the source files according to converter implementation

        Args:
            **kwargs: Additional arguments specific to the converter implementation

        Returns:
            Dictionary containing information about the conversion process
        """
        pass
