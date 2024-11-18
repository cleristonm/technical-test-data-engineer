import pytest
from datetime import datetime
from src.transformers.users_transformer import UsersTransformer
from typing import List, Dict, Any

@pytest.fixture
def transformer():
    return UsersTransformer()

@pytest.fixture
def valid_user():
    return {
        'id': 1,
        'first_name': 'Marc-Antoine',
        'last_name': 'Gagnon',
        'email': 'marc-antoine@example.com',
        'gender': 'Male',
        'favorite_genres': 'rock,pop',
        'created_at': '2024-03-20T10:00:00',
        'updated_at': '2024-03-20T10:00:00'
    }

class TestUsersTransformer:
    """Test suite for UsersTransformer"""

    def test_transform_valid_user(self, transformer, valid_user, caplog):
        """Test transformation of valid user data"""
        result = transformer._transform([valid_user])
        
        assert len(result) == 1
        transformed = result[0]
        assert transformed['id'] == valid_user['id']
        assert transformed['email'] == valid_user['email'].lower()
        assert "Starting user data transformation" in caplog.text
        assert "Successfully processed: 1" in caplog.text

    def test_validate_gender(self, transformer):
        """Test gender validation"""
        assert transformer._validate_gender("Male") == "Male"
        assert transformer._validate_gender("Agender") == "Agender"
        
        with pytest.raises(ValueError) as exc_info:
            transformer._validate_gender("invalid")
        assert "Invalid gender value" in str(exc_info.value)

    def test_duplicate_email_handling(self, transformer, valid_user, caplog):
        """Test handling of duplicate emails"""
        duplicate_user = valid_user.copy()
        duplicate_user['id'] = 2
        
        result = transformer._transform([valid_user, duplicate_user])
        
        assert len(result) == 1
        assert "Skipping duplicate email" in caplog.text

    def test_exceptions(self, transformer, caplog):
        """Test exception handling"""
        invalid_user = {
            'created_at': '2024-03-20T10:00:00',
            'updated_at': '2024-03-20T10:00:00'
        }
        
        result = transformer._transform([invalid_user])
        
        assert len(result) == 0
        expected_fields = ['id', 'first_name', 'last_name', 'email', 'gender', 'favorite_genres']
        assert f"Missing required fields: {expected_fields}" in caplog.text
        assert "Error count: 1" in caplog.text