"""
Base repository interfaces for database access.

This module defines abstract base classes that serve as interfaces
for the concrete repository implementations.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Union, Optional


class Repository(ABC):
    """
    Base interface for all repositories.

    This abstract class defines the common contract that all
    repositories must implement. It follows the Repository pattern
    by providing a standard interface for data access operations.
    """

    @abstractmethod
    def get_all(
        self, as_df: bool = True
    ) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
        """
        Get all records from the repository.

        Args:
            as_df: If True, return data as pandas DataFrame,
                  otherwise as a list of dictionaries

        Returns:
            Either a pandas DataFrame or a list of dictionaries,
            depending on the as_df parameter
        """
        pass
