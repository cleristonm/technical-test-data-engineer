from .base_transformer import BaseTransformer
from typing import List, Dict, Any

class ListenHistoryTransformer(BaseTransformer):
    """Transforms user listening history data into a standardized format."""
    
    def _transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms raw listening history data into standardized records.
        
        Args:
            raw_data: List of listening history records
            
        Returns:
            List of transformed listening history records
        """
        self.log.info(f"Starting listen history transformation. Records count: {len(raw_data)}")
        self._validate_input_type(raw_data, "histories")
            
        transformed_records = []
        validation_errors = []
        processed_count = 0
        error_count = 0
        
        for history in raw_data:
            try:
                self._validate_required_fields(history, [
                    'user_id', 'items', 'created_at', 'updated_at'
                ])
                
                if not isinstance(history['user_id'], int):
                    raise ValueError(f"Invalid user_id format: {history['user_id']}")
                    
                if not isinstance(history['items'], list):
                    raise ValueError(f"Invalid items format: {history['items']}")
                
                for track_id in history['items']:
                    if not isinstance(track_id, int) or track_id < 0:
                        error_msg = f"Invalid track ID {track_id} for user {history['user_id']}"
                        validation_errors.append(error_msg)
                        self.log.warning(error_msg)
                        error_count += 1
                        continue
                        
                    record = {
                        'user_id': history['user_id'],
                        'track_id': track_id,
                        'created_at': self._validate_timestamp(history['created_at'], 'created_at', validation_errors),
                        'updated_at': self._validate_timestamp(history['updated_at'], 'updated_at', validation_errors)
                    }
                    
                    if all(record.values()):
                        transformed_records.append(record)
                        processed_count += 1
                    else:
                        error_count += 1
                        
            except (KeyError, ValueError) as e:
                error_msg = f"Error processing history record: {str(e)}"
                self.log.error(error_msg)
                validation_errors.append(error_msg)
                error_count += 1
                
        self._log_transformation_summary(
            len(raw_data),
            processed_count,
            error_count,
            validation_errors,
            "histories"
        )
        
        return transformed_records