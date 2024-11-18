"""Unit tests for BaseTransformer class.

This module contains test cases for the BaseTransformer class, validating its
transformation, validation, and error handling capabilities.
"""

import pytest
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
from src.transformers.base_transformer import BaseTransformer, TransformerError


class TestTransformer(BaseTransformer):
    """Concrete implementation of BaseTransformer for testing."""
    
    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Simple transformation for testing."""
        return df


@pytest.fixture
def transformer():
    """Fixture providing TestTransformer instance."""
    return TestTransformer()


class TestBaseTransformer:
    """Test suite for BaseTransformer class."""

    def test_empty_data(self, transformer):
        """
        Test transformation of empty data.
        
        Args:
            transformer: TestTransformer fixture
        """
        assert transformer.transform([]) == []

    def test_timestamp_validation(self, transformer):
        """
        Test timestamp validation functionality.
        
        Args:
            transformer: TestTransformer fixture
        """
        data = [
            {'date': '2024-01-01T00:00:00', 'value': 1},
            {'date': 'invalid', 'value': 2},
            {'date': None, 'value': 3}
        ]
        df = pd.DataFrame(data)
        
        result = transformer._validate_timestamps(df, ['date'])
        
        assert len(transformer._validation_errors) == 2
        assert pd.isna(result.loc[1, 'date'])
        assert pd.isna(result.loc[2, 'date']) 