"""
Base transformer module providing common transformation functionality.

This module defines the base classes and utilities for data transformation,
including validation, error handling, and logging capabilities.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException


class TransformerError(Exception):
    """Custom exception for transformer-specific errors."""
    pass


class BaseTransformer(ABC, LoggingMixin):
    """
    Abstract base class for data transformers.
    
    Provides common functionality for data validation, transformation,
    and error handling. All transformer implementations should inherit
    from this class.
    """

    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the extracted data.
        
        Args:
            data: List of records to transform
            
        Returns:
            List of transformed records
        """
        pass

    def _validate_input_type(self, raw_data: Any, expected_type: str = "histories") -> None:
        """
        Validate input data type.
        
        Args:
            raw_data: Data to validate
            expected_type: Expected type name for error messages
            
        Raises:
            ValueError: If input is not a list
        """
        if not isinstance(raw_data, list):
            error_msg = f"Expected list of {expected_type}, got {type(raw_data).__name__}"
            self.log.error(error_msg)
            raise ValueError(error_msg)

    def _validate_string(self, value: str, field_name: str) -> str:
        """
        Validate and clean string fields.
        
        Args:
            value: String to validate
            field_name: Field name for error messages
            
        Returns:
            Cleaned string value
            
        Raises:
            ValueError: If value is not a non-empty string
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Invalid {field_name}: must be non-empty string")
        return value.strip()

    def _validate_timestamp(self, timestamp: str, field_name: str, 
                          validation_errors: List[str] = None) -> str:
        """
        Validate timestamp format.
        
        Args:
            timestamp: ISO format timestamp string
            field_name: Field name for error messages
            validation_errors: Optional list to collect validation errors
            
        Returns:
            Validated timestamp string
            
        Raises:
            ValueError: If timestamp format is invalid (unless validation_errors is provided)
        """
        try:
            if not timestamp or not isinstance(timestamp, str):
                raise ValueError(f"Invalid {field_name}: must be non-empty string")
                
            if 'T' not in timestamp:
                raise ValueError(f"Invalid {field_name} format: must include both date and time")
                
            datetime.fromisoformat(timestamp)
            return timestamp
            
        except ValueError as e:
            error_msg = f"Invalid {field_name} format: {timestamp}"
            if validation_errors is not None:
                validation_errors.append(error_msg)
                self.log.warning(error_msg)
                return False
            raise ValueError(error_msg)

    def _parse_genres(self, genres: str) -> List[str]:
        """
        Remove curly braces from genres string.
        
        Args:
            genres: String containing genres
            
        Returns:
            Genres string without curly braces
        """
        return genres.replace("{", "").replace("}", "").strip()

    def _log_transformation_summary(self, input_count: int, processed_count: int,
                                 error_count: int, validation_errors: List[str],
                                 record_type: str = "records") -> None:
        """
        Log transformation summary statistics.
        
        Args:
            input_count: Number of input records
            processed_count: Number of successfully processed records
            error_count: Number of records with errors
            validation_errors: List of validation error messages
            record_type: Type of records being processed (for logging)
        """
        self.log.info(
            f"Transformation completed:\n"
            f"- Input {record_type}: {input_count}\n"
            f"- Successfully processed: {processed_count}\n"
            f"- Error count: {error_count}\n"
            f"- Total validation errors: {len(validation_errors)}"
        )
        
        if validation_errors:
            self.log.warning("Validation errors occurred:\n" + "\n".join(validation_errors))

    def _validate_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> None:
        """
        Validate presence of required fields.
        
        Args:
            data: Dictionary containing record data
            required_fields: List of required field names
            
        Raises:
            KeyError: If any required fields are missing
        """
        if missing := [f for f in required_fields if f not in data]:
            raise KeyError(f"Missing required fields: {missing}")

    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform input data and validate output size.
        
        Args:
            data: List of input records
            
        Returns:
            List of transformed records
            
        Raises:
            AirflowException: If output size is less than 90% of input size
        """
        if not data:
            return []
            
        input_size = len(data)
        transformed_data = self._transform(data)
        output_size = len(transformed_data)
        
        if output_size < (input_size * 0.9):
            raise AirflowException(
                f"Transform validation failed: Output size ({output_size}) is less than "
                f"90% of input size ({input_size}). Lost {input_size - output_size} records."
            )
            
        return transformed_data