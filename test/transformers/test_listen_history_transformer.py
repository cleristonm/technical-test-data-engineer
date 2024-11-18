import pytest
from datetime import datetime
from src.transformers.listen_history_transformer import ListenHistoryTransformer
from typing import List, Dict, Any

@pytest.fixture
def transformer():
    return ListenHistoryTransformer()

@pytest.fixture
def valid_history():
    return {
        'user_id': 1,
        'items': [101, 102],
        'created_at': '2024-03-20T10:00:00',
        'updated_at': '2024-03-20T10:00:00'
    }

class TestListenHistoryTransformer:
    """Test suite for ListenHistoryTransformer"""

    def test_transform_valid_history(self, transformer, valid_history, caplog):
        """Test transformation of valid history data"""
        result = transformer.transform([valid_history])
        
        assert len(result) == 2  # Two tracks
        assert all(record['user_id'] == 1 for record in result)
        assert result[0]['track_id'] == 101
        assert result[1]['track_id'] == 102
        assert "Successfully processed: 2" in caplog.text

    def test_invalid_user_id(self, transformer, valid_history, caplog):
        """Test handling of invalid user_id"""
        invalid_history = valid_history.copy()
        invalid_history['user_id'] = 'not_an_int'
        
        result = transformer.transform([invalid_history])
        
        assert len(result) == 0
        assert "Invalid user_id format" in caplog.text

    def test_invalid_items_format(self, transformer, valid_history, caplog):
        """Test handling of invalid items format"""
        invalid_history = valid_history.copy()
        invalid_history['items'] = 'not_a_list'
        
        result = transformer.transform([invalid_history])
        
        assert len(result) == 0
        assert "Invalid items format" in caplog.text

    def test_invalid_track_ids(self, transformer, valid_history, caplog):
        """Test handling of invalid track IDs"""
        invalid_history = valid_history.copy()
        invalid_history['items'] = [101, -1, 'invalid', None, 102]
        
        result = transformer.transform([invalid_history])
        
        assert len(result) == 2  # Only valid track IDs should be processed
        assert "Invalid track ID" in caplog.text
        assert any(record['track_id'] == 101 for record in result)
        assert any(record['track_id'] == 102 for record in result)

    def test_complete_transformation(self, transformer, valid_history, caplog):
        """Test complete transformation with mixed valid/invalid data"""
        mixed_data = [
            valid_history,  # Valid record
            {**valid_history, 'user_id': 'invalid'},  # Invalid user_id
            {  # Valid record with some invalid tracks
                'user_id': 2,
                'items': [201, -1, 202],
                'created_at': '2024-03-20T10:00:00',
                'updated_at': '2024-03-20T10:00:00'
            }
        ]
        
        result = transformer.transform(mixed_data)
        
        assert len(result) == 4  # 2 tracks from first record + 2 valid tracks from third
        assert "Successfully processed: 4" in caplog.text
        assert "Error count:" in caplog.text

    

    