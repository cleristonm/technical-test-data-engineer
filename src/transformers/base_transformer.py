from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

class BaseTransformer(ABC, LoggingMixin):
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform the extracted data"""
        pass

    def _validate_input_type(self, raw_data: Any, expected_type: str = "histories") -> None:
        """Validate input data type"""
        if not isinstance(raw_data, list):
            error_msg = f"Expected list of {expected_type}, got {type(raw_data).__name__}"
            self.log.error(error_msg)
            raise ValueError(error_msg)

    def _validate_string(self, value: str, field_name: str) -> str:
        """Validate and clean string fields."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Invalid {field_name}: must be non-empty string")
        return value.strip()

    def _validate_timestamp(self, timestamp: str, field_name: str, validation_errors: List[str] = None) -> str:
        """Validate timestamp format."""
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
        """Parse genres string into list."""
        if not genres:
            return []
        return [genre.strip() for genre in genres.split(',') if genre.strip()]

    def _log_transformation_summary(self, 
                                 input_count: int, 
                                 processed_count: int, 
                                 error_count: int,
                                 validation_errors: List[str],
                                 record_type: str = "records") -> None:
        """Log transformation summary statistics"""
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
        """Validate presence of required fields"""
        if missing := [f for f in required_fields if f not in data]:
            raise KeyError(f"Missing required fields: {missing}")