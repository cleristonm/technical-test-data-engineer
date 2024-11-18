import pytest
from datetime import datetime
from src.transformers.base_transformer import BaseTransformer
from typing import List, Dict, Any

# Concrete class for testing abstract base
class TestTransformer(BaseTransformer):
    def transform(self, data):
        return data

@pytest.fixture
def transformer():
    return TestTransformer()

class TestBaseTransformer:
    """Test suite for BaseTransformer methods"""

    class TestValidateString:
        @pytest.mark.parametrize("value,field_name,expected", [
            ("test", "field", "test"),
            ("  padded  ", "field", "padded"),
            ("single", "field", "single"),
        ])
        def test_valid_strings(self, transformer, value, field_name, expected):
            assert transformer._validate_string(value, field_name) == expected

        @pytest.mark.parametrize("value,field_name", [
            ("", "field"),
            (None, "field"),
            (123, "field"),
            ("   ", "field"),
        ])
        def test_invalid_strings(self, transformer, value, field_name):
            with pytest.raises(ValueError) as exc_info:
                transformer._validate_string(value, field_name)
            assert f"Invalid {field_name}" in str(exc_info.value)

    class TestValidateTimestamp:
        @pytest.mark.parametrize("timestamp,expected", [
            ("2024-03-20T10:00:00", "2024-03-20T10:00:00"),
            ("2024-03-20T10:00:00+00:00", "2024-03-20T10:00:00+00:00"),
        ])
        def test_valid_timestamps(self, transformer, timestamp, expected):
            assert transformer._validate_timestamp(timestamp, "test") == expected

        @pytest.mark.parametrize("timestamp", [
            "2024-03-20",
            "10:00:00",
            "invalid",
            "",
            None,
        ])
        def test_invalid_timestamps(self, transformer, timestamp):
            with pytest.raises(ValueError):
                transformer._validate_timestamp(timestamp, "test") 