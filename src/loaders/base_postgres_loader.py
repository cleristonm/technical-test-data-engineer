"""
Base abstract class for PostgreSQL data loaders.

This module provides the base interface for implementing PostgreSQL data loaders
with common logging functionality.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from airflow.utils.log.logging_mixin import LoggingMixin


class BasePostgresLoader(LoggingMixin, ABC):
    """
    Abstract base class for PostgreSQL data loaders.

    Provides a common interface for implementing data loading operations
    into PostgreSQL databases with integrated logging capabilities.
    """

    @abstractmethod
    def load(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Abstract method to load data into a PostgreSQL table.

        Args:
            table_name: Name of the target table
            data: List of dictionaries containing the data to be loaded

        Raises:
            NotImplementedError: When called directly on the base class
        """
        pass 