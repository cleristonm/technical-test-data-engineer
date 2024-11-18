"""
Base transformer module for data processing using Pandas.

This module provides common transformation functionality for all data types,
implementing shared validation and processing methods.
"""

import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin


class TransformerError(Exception):
    """Custom exception for transformer-specific errors."""

    pass


class BaseTransformer(ABC, LoggingMixin):
    """
    Abstract base class for data transformers using Pandas.

    This class provides common functionality for data validation and transformation
    that can be shared across different transformer implementations.

    Attributes:
        _validation_errors (List[str]): Collection of validation error messages
    """

    def __init__(self):
        """Initialize base transformer with validation error collection."""
        super().__init__()
        self._validation_errors = []

    @abstractmethod
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the input DataFrame.

        Args:
            df: Input DataFrame to transform

        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        pass

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform input data and ensure JSON serializable output.

        Args:
            data: List of dictionaries to transform

        Returns:
            List[Dict[str, Any]]: Transformed records with serializable values

        Raises:
            TransformerError: If transformation fails
        """
        if not data:
            return []

        try:
            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Apply transformation
            df = self._transform(df)

            # Convert timestamps to ISO format strings
            for col in df.select_dtypes(include=["datetime64[ns]"]).columns:
                df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            # Convert to records
            return df.to_dict("records")

        except Exception as e:
            self.log.error(f"Transformation failed: {str(e)}")
            raise TransformerError(str(e)) from e

    def _validate_timestamps(
        self, df: pd.DataFrame, timestamp_columns: List[str]
    ) -> pd.DataFrame:
        """
        Validate timestamp columns in DataFrame.

        Args:
            df: Input DataFrame
            timestamp_columns: List of column names containing timestamps

        Returns:
            pd.DataFrame: DataFrame with validated timestamps
        """
        for col in timestamp_columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            invalid_dates = df[col].isna()
            if invalid_dates.any():
                self._validation_errors.extend(
                    f"Invalid {col} format at index {idx}"
                    for idx in df[invalid_dates].index
                )
        return df

    def _validate_string_columns(
        self, df: pd.DataFrame, string_columns: List[str]
    ) -> pd.DataFrame:
        """
        Validate and clean string columns.

        Args:
            df: Input DataFrame
            string_columns: List of column names containing strings

        Returns:
            pd.DataFrame: DataFrame with cleaned strings
        """
        for col in string_columns:
            df[col] = df[col].str.strip()
            invalid_strings = df[col].isna() | (df[col] == "")
            if invalid_strings.any():
                self._validation_errors.extend(
                    f"Invalid {col} at index {idx}" for idx in df[invalid_strings].index
                )
        return df

    def _log_transformation_summary(
        self, df: pd.DataFrame, record_type: str = "records"
    ) -> None:
        """
        Log transformation summary statistics.

        Args:
            df: Transformed DataFrame
            record_type: Type of records being processed
        """
        self.log.info(
            f"Transformation completed:\n"
            f"- Input {record_type}: {len(df)}\n"
            f"- Successfully processed: {len(df.dropna())}\n"
            f"- Error count: {len(self._validation_errors)}\n"
            f"- Validation errors: {len(self._validation_errors)}"
        )

        if self._validation_errors:
            self.log.warning(
                "Validation errors occurred:\n" + "\n".join(self._validation_errors)
            )
