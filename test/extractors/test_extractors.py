import pytest
import requests
from unittest.mock import Mock, patch
from src.extractors.generic_extractor import GenericExtractor

@pytest.fixture
def extractor():
    """Create a GenericExtractor instance for testing."""
    return GenericExtractor("http://test.api", "test-endpoint")

@pytest.fixture
def mock_single_page():
    """Mock response with a single page of data."""
    response = Mock()
    response.json.return_value = {
        'items': [{'id': 1}, {'id': 2}],
        'pages': 1
    }
    return response

@pytest.fixture
def mock_multiple_pages():
    """Mock responses for multiple pages of data."""
    return [
        Mock(json=lambda: {'items': [{'id': 1}], 'pages': 2}),
        Mock(json=lambda: {'items': [{'id': 2}], 'pages': 2})
    ]

class TestExtractors:
    """Test suite for BaseExtractor and GenericExtractor."""

    def test_successful_single_page(self, extractor, mock_single_page):
        """Test successful extraction of a single page."""
        with patch('requests.get', return_value=mock_single_page) as mock_get:
            result = extractor.extract()
            
            assert len(result) == 2
            assert result[0]['id'] == 1
            assert result[1]['id'] == 2
            mock_get.assert_called_once_with(
                'http://test.api/test-endpoint?page=1&size=100',
                timeout=30
            )

    def test_successful_multiple_pages(self, extractor, mock_multiple_pages):
        """Test successful extraction of multiple pages."""
        with patch('requests.get', side_effect=mock_multiple_pages) as mock_get:
            result = extractor.extract()
            
            assert len(result) == 2
            assert result[0]['id'] == 1
            assert result[1]['id'] == 2
            assert mock_get.call_count == 2

    def test_http_error(self, extractor):
        """Test handling of HTTP errors."""
        with patch('requests.get', side_effect=requests.exceptions.HTTPError("404 Error")):
            with pytest.raises(Exception, match="API request failed"):
                extractor.extract()

    def test_invalid_response_format(self, extractor):
        """Test handling of invalid API response format."""
        mock_response = Mock()
        mock_response.json.return_value = {'invalid': 'format'}
        
        with patch('requests.get', return_value=mock_response):
            with pytest.raises(Exception, match="Invalid data format"):
                extractor.extract()

    def test_custom_page_size(self, extractor, mock_single_page):
        """Test extraction with custom page size."""
        with patch('requests.get', return_value=mock_single_page) as mock_get:
            extractor.fetch_all_pages(page_size=50)
            mock_get.assert_called_once_with(
                'http://test.api/test-endpoint?page=1&size=50',
                timeout=30
            ) 