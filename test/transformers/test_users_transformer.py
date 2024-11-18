"""Unit tests for UsersTransformer class.

This module contains test cases for the UsersTransformer class, validating its
user data transformation and validation capabilities.
"""

import pytest
from src.transformers.users_transformer import UsersTransformer, TransformerError


@pytest.fixture
def transformer():
    """Fixture providing UsersTransformer instance."""
    return UsersTransformer()


@pytest.fixture
def valid_user_data():
    """Fixture providing valid user test data."""
    return [
        {
            "id": 1,
            "first_name": "John",
            "last_name": "Doe",
            "email": "john@example.com",
            "gender": "Male",
            "favorite_genres": "{Rock}",
            "created_at": "2024-01-01T00:00:00",
            "updated_at": "2024-01-01T00:00:00",
        }
    ]


class TestUsersTransformer:
    """Test suite for UsersTransformer class."""

    def test_valid_user_transformation(self, transformer, valid_user_data):
        """
        Test transformation of valid user data.

        Args:
            transformer: UsersTransformer fixture
            valid_user_data: Valid user data fixture
        """
        result = transformer.transform(valid_user_data)

        assert len(result) == 1
        assert result[0]["email"] == "john@example.com"
        assert result[0]["gender"] == "Male"
        assert result[0]["favorite_genres"] == "Rock"

    def test_duplicate_email_handling(self, transformer):
        """
        Test handling of duplicate email addresses.

        Args:
            transformer: UsersTransformer fixture
        """
        data = [
            {
                "id": 1,
                "email": "duplicate@example.com",
                "first_name": "First",
                "last_name": "User",
                "gender": "Female",
                "favorite_genres": "{Rock}",
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-01T00:00:00",
            },
            {
                "id": 2,
                "email": "duplicate@example.com",
                "first_name": "Second",
                "last_name": "User",
                "gender": "Male",
                "favorite_genres": "{Pop}",
                "created_at": "2024-01-01T00:00:00",
                "updated_at": "2024-01-01T00:00:00",
            },
        ]

        result = transformer.transform(data)

        assert len(result) == 1
        assert "duplicate email found" in transformer._validation_errors[0].lower()

    def test_invalid_gender_handling(self, transformer, valid_user_data):
        """
        Test handling of invalid gender values.

        Args:
            transformer: UsersTransformer fixture
            valid_user_data: Valid user data fixture
        """
        data = valid_user_data.copy()
        data[0]["gender"] = "Invalid"

        result = transformer.transform(data)

        assert len(result) == 0
        assert "invalid gender value" in transformer._validation_errors[0].lower()

    def test_missing_required_fields(self, transformer):
        """
        Test handling of missing required fields.

        Args:
            transformer: UsersTransformer fixture
        """
        data = [{"id": 1, "email": "test@example.com"}]  # Missing required fields

        with pytest.raises(TransformerError) as exc_info:
            transformer.transform(data)

        assert "missing required fields" in str(exc_info.value).lower()

    def test_genre_formatting(self, transformer, valid_user_data):
        """
        Test formatting of favorite genres.

        Args:
            transformer: UsersTransformer fixture
            valid_user_data: Valid user data fixture
        """
        data = valid_user_data.copy()
        data[0]["favorite_genres"] = "{Rock, Pop}"

        result = transformer.transform(data)

        assert result[0]["favorite_genres"] == "Rock, Pop"
