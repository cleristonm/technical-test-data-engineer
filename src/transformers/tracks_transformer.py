from .base_transformer import BaseTransformer
from typing import List, Dict, Any
import json
from airflow.utils.log.logging_mixin import LoggingMixin


class TracksTransformer(BaseTransformer, LoggingMixin):
    def transform(self, raw_data):
        
        self.log.info(f"Raw data type: {type(raw_data).__name__}")
        
        # Now process each track
        transformed_tracks = []
        for track in raw_data:
            try:
                transformed_track = {
                    'id': track['id'],
                    'name': track['name'],
                    'artist': track['artist'],
                    'duration': track['duration'],
                    'genres': track['genres'].split(',') if track['genres'] else [],
                    'created_at': track['created_at'],
                    'updated_at': track['updated_at']
                }
                transformed_tracks.append(transformed_track)
                
            except Exception as e:
                error_msg = f"Unexpected error processing track"
                self.log.error(error_msg)
                self.log.error(f"Track data: {track}")
                raise
                return transformed_tracks
        
        self.log.info(f"Successfully transformed {len(transformed_tracks)} tracks")
        return transformed_tracks