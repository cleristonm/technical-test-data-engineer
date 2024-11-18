"""Unit tests for GenericPostgresLoader."""

from unittest.mock import MagicMock, patch
import pytest
import psycopg2
from src.loaders.generic_postgres_loader import GenericPostgresLoader


@pytest.fixture
def connection_params():
    """Return test database connection parameters."""
    return {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }


@pytest.fixture
def loader(connection_params):
    """Return GenericPostgresLoader instance."""
    return GenericPostgresLoader(connection_params)


@pytest.fixture
def sample_data():
    """Return test records."""
    return [
        {'id': 1, 'name': 'Test 1', 'value': 100},
        {'id': 2, 'name': 'Test 2', 'value': 200}
    ]


@pytest.fixture
def mock_db():
    """Return mocked database components."""
    with patch('psycopg2.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.connection.encoding = 'UTF8'
        yield {
            'cursor': mock_cursor,
            'connection': mock_connection,
            'connect': mock_connect
        }


@patch('src.loaders.generic_postgres_loader.execute_values')
class TestGenericPostgresLoader:
    """Test GenericPostgresLoader functionality."""

    def test_empty_data(self, mock_execute_values, loader):
        """Verify loader skips processing for empty data."""
        loader.load('test_table', [])
        mock_execute_values.assert_not_called()

    def test_database_error(self, mock_execute_values, loader, sample_data):
        """Verify loader handles database errors appropriately."""
        with patch('psycopg2.connect', side_effect=psycopg2.Error("Connection failed")):
            with pytest.raises(psycopg2.Error, match="Connection failed"):
                loader.load('test_table', sample_data)
        mock_execute_values.assert_not_called()

    def test_data_loading(self, mock_execute_values, loader, sample_data, mock_db):
        """Verify loader processes data correctly."""
        loader.load('test_table', sample_data)

        mock_execute_values.assert_called_once()
        
        cursor_arg = mock_execute_values.call_args[0][0]
        query = mock_execute_values.call_args[0][1]
        values = mock_execute_values.call_args[0][2]

        assert cursor_arg == mock_db['cursor']
        assert 'INSERT INTO test_table' in query
        assert 'ON CONFLICT (id) DO UPDATE' in query
        assert values == [[1, 'Test 1', 100], [2, 'Test 2', 200]]

    

    
