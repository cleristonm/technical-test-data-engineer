"""
Unit tests for the TracksTransformer class.

This module contains test cases for validating the transformation logic
of music track data, considering Airflow XCom compatibility.
"""

import pytest
from src.transformers.tracks_transformer import TracksTransformer, TransformerError


@pytest.fixture
def transformer():
    """
    Create a TracksTransformer instance for testing.

    Returns:
        TracksTransformer: A new transformer instance for each test.
    """
    return TracksTransformer()


@pytest.fixture
def valid_track_data():
    """
    Create sample valid track data for testing.

    Returns:
        list: List containing valid track records for XCom.
    """
    return [
        {
            'id': '1',
            'name': 'Test Track',
            'artist': 'Test Artist',
            'duration': '3:45',
            'genres': '{rock,pop}',
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z'
        }
    ]


def test_successful_transformation(transformer, valid_track_data):
    """
    Test successful transformation of valid track data.

    Args:
        transformer: TracksTransformer fixture.
        valid_track_data: List fixture with valid track data.
    """
    result = transformer.transform(valid_track_data)
    
    assert isinstance(result, list)
    assert len(result) == 1
    
    # Verify transformed data
    first_record = result[0]
    assert first_record['id'] == 1
    assert first_record['name'] == 'Test Track'
    assert first_record['artist'] == 'Test Artist'
    assert first_record['duration'] == '3:45'
    assert first_record['genres'] == 'rock,pop'


def test_missing_required_fields(transformer):
    """
    Test transformation with missing required fields.

    Args:
        transformer: TracksTransformer fixture.
    """
    invalid_data = [{'id': 1, 'name': 'Test Track'}]  # Missing required fields
    
    with pytest.raises(TransformerError) as exc_info:
        transformer.transform(invalid_data)
    assert "Missing required fields" in str(exc_info.value)


def test_invalid_duration_format(transformer):
    """
    Test transformation with invalid duration format.

    Args:
        transformer: TracksTransformer fixture.
    """
    invalid_data = [{
        'id': '1',
        'name': 'Test Track',
        'artist': 'Test Artist',
        'duration': 'invalid',  # Invalid duration
        'genres': '{rock}',
        'created_at': '2024-01-01T00:00:00Z',
        'updated_at': '2024-01-01T00:00:00Z'
    }]
    
    result = transformer.transform(invalid_data)
    assert len(result) == 0  # Verify invalid records are filtered out
    