from .base_transformer import BaseTransformer
from typing import List, Dict, Any
import json
from airflow.utils.log.logging_mixin import LoggingMixin


class TracksTransformer(BaseTransformer, LoggingMixin):
    """
    Transformer for processing music track data.
    
    This class handles the transformation of raw track data into a standardized format,
    including data validation, error handling, and genre parsing.
    
    Attributes:
        None
        
    Raises:
        ValueError: When required fields are missing or have invalid format
        KeyError: When attempting to access missing track data fields
        Exception: For general transformation errors
    """
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform raw track data into standardized format.
        
        Args:
            raw_data (List[Dict[str, Any]]): List of raw track records containing:
                - id (int): Track identifier
                - name (str): Track name
                - artist (str): Artist name
                - duration (str): Track duration in format 'MM:SS'
                - genres (str): Comma-separated list of genres
                - created_at (str): Creation timestamp
                - updated_at (str): Last update timestamp
                
        Returns:
            List[Dict[str, Any]]: List of transformed track records with standardized format
            
        Raises:
            ValueError: If data validation fails
            KeyError: If required fields are missing
            Exception: For unexpected transformation errors
        """
        self.log.info(f"Starting track transformation. Data type: {type(raw_data).__name__}")
        
        if not isinstance(raw_data, list):
            error_msg = f"Expected list of tracks, got {type(raw_data).__name__}"
            self.log.error(error_msg)
            raise ValueError(error_msg)
            
        transformed_tracks = []
        validation_errors = []
        
        for index, track in enumerate(raw_data):
            try:
                # Validate required fields
                required_fields = ['id', 'name', 'artist', 'duration', 'genres', 'created_at', 'updated_at']
                missing_fields = [field for field in required_fields if field not in track]
                
                if missing_fields:
                    raise KeyError(f"Missing required fields: {missing_fields}")
                
                # Validate data types
                if not isinstance(track['id'], int):
                    raise ValueError(f"Invalid id format: {track['id']}")
                    
                if not isinstance(track['duration'], str) or ':' not in track['duration']:
                    raise ValueError(f"Invalid duration format: {track['duration']}")
                
                # Transform track data
                transformed_track = {
                    'id': track['id'],
                    'name': self._validate_string(track['name'], 'name'),
                    'artist': self._validate_string(track['artist'], 'artist'),
                    'duration': track['duration'],
                    'genres': self._parse_genres(track['genres']),
                    'created_at': self._validate_timestamp(track['created_at'], 'created_at'),
                    'updated_at': self._validate_timestamp(track['updated_at'], 'updated_at')
                }
                
                transformed_tracks.append(transformed_track)
                self.log.debug(f"Successfully transformed track {track['id']}")
                
            except (KeyError, ValueError) as e:
                error_msg = f"Error processing track at index {index}: {str(e)}"
                self.log.error(error_msg)
                validation_errors.append(error_msg)
                continue
                
            except Exception as e:
                error_msg = f"Unexpected error processing track at index {index}: {str(e)}"
                self.log.error(error_msg)
                self.log.error(f"Track data: {track}")
                raise Exception(error_msg) from e
        
        # Log summary statistics
        self.log.info(
            f"Transformation completed:\n"
            f"- Successfully processed: {len(transformed_tracks)} tracks\n"
            f"- Validation errors: {len(validation_errors)}"
        )
        
        if validation_errors:
            self.log.warning("Validation errors occurred:\n" + "\n".join(validation_errors))
            
        return transformed_tracks

    def _validate_string(self, value: str, field_name: str) -> str:
        """
        Validate and clean string fields.
        
        Args:
            value (str): String value to validate
            field_name (str): Name of the field being validated
            
        Returns:
            str: Cleaned string value
            
        Raises:
            ValueError: If value is empty or not a string
        """
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Invalid {field_name}: must be non-empty string")
        return value.strip()

    def _parse_genres(self, genres: str) -> List[str]:
        """
        Parse genres string into list of genres.
        
        Args:
            genres (str): Comma-separated string of genres
            
        Returns:
            List[str]: List of cleaned genre strings
        """
        if not genres:
            return []
        return [genre.strip() for genre in genres.split(',') if genre.strip()]

    def _validate_timestamp(self, timestamp: str, field_name: str) -> str:
        """
        Validate timestamp format.
        
        Args:
            timestamp (str): Timestamp string to validate
            field_name (str): Name of the timestamp field
            
        Returns:
            str: Validated timestamp string
            
        Raises:
            ValueError: If timestamp format is invalid
        """
        if not timestamp or not isinstance(timestamp, str):
            raise ValueError(f"Invalid {field_name}: must be non-empty string")
        try:
            # You might want to add more specific timestamp validation here
            return timestamp
        except Exception as e:
            raise ValueError(f"Invalid {field_name} format: {timestamp}") from e