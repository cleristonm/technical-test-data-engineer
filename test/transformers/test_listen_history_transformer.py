"""
Unit tests for the ListenHistoryTransformer class.

This module contains test cases for validating the transformation logic
of listening history data using pandas DataFrames.
"""

import pytest
from src.transformers.listen_history_transformer import ListenHistoryTransformer, TransformerError


@pytest.fixture
def transformer():
    """
    Fixture that provides a ListenHistoryTransformer instance.
    
    Returns:
        ListenHistoryTransformer: A new transformer instance for each test
    """
    return ListenHistoryTransformer()


@pytest.fixture
def valid_data():
    """
    Fixture that provides valid listening history data.
    
    Returns:
        list: A list of dictionaries containing valid listening history records
    """
    return [
        {
            'user_id': '1',
            'items': [101, 102, 103],
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
    ]


def test_successful_transformation(transformer, valid_data):
    """
    Test successful transformation of valid listening history data.
    """
    result = transformer.transform(valid_data)
    
    assert isinstance(result, list)  # Expecting a list for XCom
    assert len(result) == 3  # One row per track in items
    
    # Check structure of first record
    first_record = result[0]
    expected_keys = {'user_id', 'track_id', 'created_at', 'updated_at'}
    assert all(key in first_record for key in expected_keys)
    
    assert first_record['user_id'] == 1
    assert sorted(record['track_id'] for record in result) == [101, 102, 103]


def test_missing_required_fields(transformer):
    """
    Test transformation with missing required fields raises TransformerError.
    
    Args:
        transformer: ListenHistoryTransformer fixture
    """
    invalid_data = [{'user_id': '1', 'items': [101]}]  # Missing timestamps
    
    with pytest.raises(TransformerError) as exc_info:
        transformer.transform(invalid_data)
    
    assert "Missing required fields" in str(exc_info.value)

def test_invalid_items_format(transformer):
    """
    Test transformation with invalid items format.
    
    Args:
        transformer: ListenHistoryTransformer fixture
    """
    invalid_data = [
        {
            'user_id': '1',
            'items': 'not_a_list',  # Invalid items format
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
    ]
    
    result = transformer.transform(invalid_data)
    assert len(result) == 0  # Should return empty DataFrame
    assert "Invalid items format" in transformer._validation_errors[0]


def test_invalid_track_ids(transformer):
    """
    Test transformation with invalid track IDs.
    """
    invalid_data = [
        {
            'user_id': '1',
            'items': [101, 'invalid', -1],
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
    ]
    
    result = transformer.transform(invalid_data)
    assert isinstance(result, list)
    assert len(result) == 1  # Only valid track_id should remain
    assert result[0]['track_id'] == 101


def test_invalid_timestamps(transformer):
    """
    Test transformation with invalid timestamp formats.
    
    Args:
        transformer: ListenHistoryTransformer fixture
    """
    invalid_data = [
        {
            'user_id': '1',
            'items': [101],
            'created_at': 'invalid_date',
            'updated_at': '2024-01-01T00:00:00Z'
        }
    ]
    
    result = transformer.transform(invalid_data)
    assert len(result) == 0  # Invalid timestamps should be filtered out


def test_empty_input(transformer):
    """
    Test transformation with empty input data.
    """
    result = transformer.transform([])
    assert isinstance(result, list)
    assert len(result) == 0