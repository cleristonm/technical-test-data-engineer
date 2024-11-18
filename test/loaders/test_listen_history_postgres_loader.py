from unittest.mock import MagicMock, patch, call
import pytest
import psycopg2
from src.loaders.listen_history_postgres_loader import ListenHistoryPostgresLoader

@pytest.fixture
def sample_data():
    """Provide sample listen history data for testing."""
    return [
        {
            'user_id': 1,
            'track_id': 100,
            'updated_at': '2024-03-20T10:00:00'
        },
        {
            'user_id': 2,
            'track_id': 200,
            'updated_at': '2024-03-20T11:00:00'
        }
    ]

@pytest.fixture
def loader():
    """Provide a configured ListenHistoryPostgresLoader instance."""
    connection_params = {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'test_user',
        'password': 'test_pass'
    }
    return ListenHistoryPostgresLoader(connection_params)

@pytest.fixture
def mock_db():
    """Provide mocked database connection and cursor."""
    with patch('psycopg2.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.connection.encoding = 'UTF8'
        yield {
            'connect': mock_connect,
            'connection': mock_connection,
            'cursor': mock_cursor
        }

@patch('src.loaders.listen_history_postgres_loader.execute_values')
class TestListenHistoryPostgresLoader:
    """Test suite for ListenHistoryPostgresLoader class."""

    def test_empty_data(self, mock_execute_values, loader):
        """Verify that loader handles empty data correctly."""
        loader.load('listen_history', [])
        mock_execute_values.assert_not_called()

    def test_valid_user_ids(self, mock_execute_values, loader, sample_data, mock_db):
        """Verify successful data loading with valid user IDs."""
        mock_db['cursor'].fetchall.return_value = [(1,), (2,)]

        loader.load('listen_history', sample_data)

        # Verify user validation query
        validation_calls = [
            call for call in mock_db['cursor'].execute.call_args_list 
            if "SELECT id FROM users" in call[0][0]
        ]
        assert len(validation_calls) == 1

        # Verify data insertion
        mock_execute_values.assert_called_once()
        insert_query = mock_execute_values.call_args[0][1]
        assert "INSERT INTO listen_history" in insert_query
        assert "(user_id,track_id,updated_at)" in insert_query.replace(" ", "")

    def test_invalid_user_ids(self, mock_execute_values, loader, sample_data, mock_db):
        """Verify handling of invalid user IDs."""
        mock_db['cursor'].fetchall.return_value = [(1,)]  # Only first user exists

        loader.load('listen_history', sample_data)

        # Verify only valid records are processed
        mock_execute_values.assert_called_once()
        values = mock_execute_values.call_args[0][2]
        assert len(values) == 1
        assert values[0][0] == 1

    def test_database_error(self, mock_execute_values, loader, sample_data):
        """Verify database connection error handling."""
        with patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection failed")
            
            with pytest.raises(psycopg2.Error):
                loader.load('listen_history', sample_data)
            
            mock_execute_values.assert_not_called()

    def test_delete_existing_records(self, mock_execute_values, loader, sample_data, mock_db):
        """Verify proper deletion of existing records before insertion."""
        mock_db['cursor'].fetchall.return_value = [(1,), (2,)]

        loader.load('listen_history', sample_data)

        # Verify delete query execution
        delete_calls = [
            call for call in mock_db['cursor'].execute.call_args_list 
            if "DELETE FROM listen_history" in call[0][0]
        ]
        assert len(delete_calls) == 1

        # Verify delete parameters
        delete_params = delete_calls[0][0][1]
        assert delete_params == (
            [1, 2],  # user_ids
            [100, 200],  # track_ids
            ['2024-03-20T10:00:00', '2024-03-20T11:00:00']  # updated_ats
        )