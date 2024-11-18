import pytest
from datetime import datetime
from src.transformers.tracks_transformer import TracksTransformer
from typing import List, Dict, Any

@pytest.fixture
def transformer():
    return TracksTransformer()

@pytest.fixture
def valid_track():
    return {
        'id': 1,
        'name': 'Test Track',
        'artist': 'Test Artist',
        'duration': '03:45',
        'genres': 'rock,pop',
        'created_at': '2024-03-20T10:00:00',
        'updated_at': '2024-03-20T10:00:00'
    }

class TestTracksTransformer:
    """Test suite for TracksTransformer"""

    def test_transform_valid_track(self, transformer, valid_track, caplog):
        """Test transformation of valid track data"""
        result = transformer._transform([valid_track])
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == valid_track['id']
        assert transformed['duration'] == valid_track['duration']
        assert "Successfully processed: 1" in caplog.text

    def test_invalid_duration_format(self, transformer, valid_track, caplog):
        """Test handling of invalid duration format"""
        invalid_track = valid_track.copy()
        invalid_track['duration'] = 'invalid'
        
        result = transformer._transform([invalid_track])
        
        assert len(result) == 0
        assert "Invalid duration format" in caplog.text