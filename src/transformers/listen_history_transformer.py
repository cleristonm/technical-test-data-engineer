from .base_transformer import BaseTransformer
from typing import List, Dict, Any
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

class ListenHistoryTransformer(BaseTransformer, LoggingMixin):
    """
    Transformer for processing user listening history data with error handling.
    """
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw listening history data, continuing on errors.
        """
        self.log.info(f"Starting listen history transformation. Records count: {len(raw_data)}")
        
        if not isinstance(raw_data, list):
            error_msg = f"Expected list of histories, got {type(raw_data).__name__}"
            self.log.error(error_msg)
            raise ValueError(error_msg)
            
        transformed_records = []
        validation_errors = []
        processed_count = 0
        error_count = 0
        
        # Process each history record separately to continue on errors
        for history in raw_data:
            try:
                if not self._validate_history(history, validation_errors):
                    error_count += 1
                    continue
                
                # Process each track in the history
                for track_id in history['items']:
                    try:
                        if not self._validate_track_id(track_id, history['user_id'], validation_errors):
                            error_count += 1
                            continue
                            
                        record = {
                            'user_id': history['user_id'],
                            'track_id': track_id,
                            'created_at': history['created_at'],
                            'updated_at': history['updated_at']
                        }
                        
                        transformed_records.append(record)
                        processed_count += 1
                        
                    except Exception as e:
                        error_msg = (f"Error processing track {track_id} for user "
                                   f"{history['user_id']}: {str(e)}")
                        self.log.error(error_msg)
                        validation_errors.append(error_msg)
                        error_count += 1
                        continue
                        
            except Exception as e:
                error_msg = f"Error processing history record: {str(e)}"
                self.log.error(error_msg)
                validation_errors.append(error_msg)
                error_count += 1
                continue
        
        # Log summary statistics
        self.log.info(
            f"Transformation completed:\n"
            f"- Input histories: {len(raw_data)}\n"
            f"- Successfully processed records: {processed_count}\n"
            f"- Error count: {error_count}\n"
            f"- Total validation errors: {len(validation_errors)}"
        )
        
        if validation_errors:
            self.log.warning("Validation errors occurred:\n" + "\n".join(validation_errors))
        
        return transformed_records

    def _validate_history(self, history: Dict[str, Any], validation_errors: List[str]) -> bool:
        """Validate history record."""
        try:
            # Validate required fields
            required_fields = ['user_id', 'items', 'created_at', 'updated_at']
            if missing := [f for f in required_fields if f not in history]:
                raise KeyError(f"Missing required fields: {missing}")
                
            # Validate data types
            if not isinstance(history['user_id'], int):
                raise ValueError(f"Invalid user_id format: {history['user_id']}")
                
            if not isinstance(history['items'], list):
                raise ValueError(f"Invalid items format: {history['items']}")
                
            # Validate timestamps
            self._validate_timestamp(history['created_at'], 'created_at')
            self._validate_timestamp(history['updated_at'], 'updated_at')
            
            return True
            
        except (KeyError, ValueError) as e:
            error_msg = f"Error validating history for user {history.get('user_id', 'unknown')}: {str(e)}"
            validation_errors.append(error_msg)
            self.log.error(error_msg)
            return False

    def _validate_track_id(self, track_id: int, user_id: int, validation_errors: List[str]) -> bool:
        """Validate track ID."""
        try:
            if not isinstance(track_id, int) or track_id < 0:
                raise ValueError(f"Invalid track ID: {track_id}")
            return True
            
        except ValueError as e:
            error_msg = f"Invalid track ID {track_id} for user {user_id}: {str(e)}"
            validation_errors.append(error_msg)
            self.log.warning(error_msg)
            return False

    def _validate_timestamp(self, timestamp: str, field_name: str) -> str:
        """Validate timestamp format."""
        if not timestamp or not isinstance(timestamp, str):
            raise ValueError(f"Invalid {field_name}: must be non-empty string")
        try:
            datetime.fromisoformat(timestamp)
            return timestamp
        except ValueError as e:
            raise ValueError(f"Invalid {field_name} format: {timestamp}") from e