from .base_transformer import BaseTransformer
from typing import List, Dict, Any

class TracksTransformer(BaseTransformer):
    """Transforms music track data into a standardized format."""
    
    def _transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms raw track data into standardized records.
        
        Args:
            raw_data: List of track records containing id, name, artist,
                     duration (MM:SS), genres, created_at and updated_at
            
        Returns:
            List of transformed track records
        """
        self.log.info(f"Starting track transformation. Records count: {len(raw_data)}")
        self._validate_input_type(raw_data, "tracks")
            
        transformed_tracks = []
        validation_errors = []
        processed_count = 0
        error_count = 0
        
        for index, track in enumerate(raw_data):
            try:
                self._validate_required_fields(track, [
                    'id', 'name', 'artist', 'duration', 
                    'genres', 'created_at', 'updated_at'
                ])
                
                if not isinstance(track['id'], int):
                    raise ValueError(f"Invalid id format: {track['id']}")
                    
                if not isinstance(track['duration'], str) or ':' not in track['duration']:
                    raise ValueError(f"Invalid duration format: {track['duration']}")
                
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
                processed_count += 1
                
            except (KeyError, ValueError) as e:
                error_msg = f"Error processing track at index {index}: {str(e)}"
                self.log.error(error_msg)
                validation_errors.append(error_msg)
                error_count += 1
                
            except Exception as e:
                error_msg = f"Unexpected error processing track at index {index}: {str(e)}"
                self.log.error(error_msg)
                self.log.error(f"Track data: {track}")
                raise Exception(error_msg) from e
        
        self._log_transformation_summary(
            len(raw_data), 
            processed_count, 
            error_count, 
            validation_errors,
            "tracks"
        )
            
        return transformed_tracks