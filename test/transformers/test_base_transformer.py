"""Test suite for BaseTransformer class.

This module contains test cases for the BaseTransformer class, validating its
transformation, validation, and error handling capabilities.

Note:
    All test methods use a concrete implementation of BaseTransformer
    to verify the abstract base class functionality.
"""

import pytest
from typing import List, Dict, Any
from airflow.exceptions import AirflowException
from src.transformers.base_transformer import BaseTransformer


class TestTransformer(BaseTransformer):
    """Concrete implementation of BaseTransformer for testing purposes.
    
    Args:
        BaseTransformer: Abstract base class being tested
    """
    
    def _transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Implements the abstract transform method.
        
        Args:
            data: List of dictionaries to transform
            
        Returns:
            List[Dict[str, Any]]: Transformed data
        """
        return data


class TestBaseTransformer:
    """Test suite for BaseTransformer methods."""

    @pytest.fixture
    def transformer(self):
        """Provides a TestTransformer instance for testing.
        
        Returns:
            TestTransformer: Instance of the concrete transformer implementation
        """
        return TestTransformer()

    class TestTransform:
        """Test cases for the transform method."""

        def test_empty_input(self, transformer):
            """Test transformation of empty input.
            
            Args:
                transformer: TestTransformer fixture
            """
            assert transformer.transform([]) == []

        def test_size_validation(self, transformer):
            """Test output size validation.
            
            Args:
                transformer: TestTransformer fixture
                
            Raises:
                AirflowException: When output size is less than 90% of input
            """
            input_data = [{"id": i} for i in range(100)]
            filtered_data = [{"id": i} for i in range(80)]  # 80% of input
            
            transformer._transform = lambda x: filtered_data
            
            with pytest.raises(AirflowException) as exc_info:
                transformer.transform(input_data)
            assert "Output size (80) is less than 90% of input size (100)" in str(exc_info.value)

        def test_successful_transform(self, transformer):
            """Test successful transformation with valid data.
            
            Args:
                transformer: TestTransformer fixture
            """
            input_data = [{"id": i} for i in range(10)]
            result = transformer.transform(input_data)
            assert len(result) == len(input_data)
            assert result == input_data

    class TestValidateRequiredFields:
        """Test cases for required fields validation."""

        def test_missing_fields(self, transformer):
            """Test validation when required fields are missing.
            
            Args:
                transformer: TestTransformer fixture
                
            Raises:
                KeyError: When required fields are missing
            """
            data = {"field1": "value1"}
            required = ["field1", "field2"]
            
            with pytest.raises(KeyError) as exc_info:
                transformer._validate_required_fields(data, required)
            assert "Missing required fields" in str(exc_info.value)

        def test_all_fields_present(self, transformer):
            """Test validation when all required fields are present.
            
            Args:
                transformer: TestTransformer fixture
            """
            data = {"field1": "value1", "field2": "value2"}
            required = ["field1", "field2"]
            
            # Should not raise any exception
            transformer._validate_required_fields(data, required) 