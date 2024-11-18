"""
Base extractor module for handling API data extraction.

This module provides a base class for implementing data extractors with common
functionality for handling API requests, pagination, and error handling.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any
import requests
from airflow.utils.log.logging_mixin import LoggingMixin


class BaseExtractor(ABC, LoggingMixin):
    """Base class for data extraction from API endpoints."""

    def __init__(self, base_url: str, endpoint: str):
        """
        Initialize the extractor with API endpoint information.

        Args:
            base_url: Base URL of the API
            endpoint: Specific endpoint path
        """
        self.base_url = base_url
        self.endpoint = endpoint
        super().__init__()

    def fetch_all_pages(self, page_size: int = 100) -> List[Dict]:
        """
        Fetches all pages from the API endpoint with pagination.

        Args:
            page_size: Number of items per page

        Returns:
            List of items from all pages

        Raises:
            Exception: If API request fails or returns invalid data
        """
        all_items = []
        current_page = 1

        self.log.info(f"Starting data extraction from endpoint: {self.endpoint}")

        try:
            while True:
                url = f"{self.base_url}/{self.endpoint}?page={current_page}&size={page_size}"

                response = requests.get(url, timeout=30)
                response.raise_for_status()

                data = response.json()
                items = data["items"]
                all_items.extend(items)

                self.log.info(f"Added {len(items)} items from page {current_page}")

                if current_page >= data["pages"]:
                    self.log.info(f"Reached last page ({current_page})")
                    break

                current_page += 1

            self.log.info(
                f"Extraction completed. Total items fetched: {len(all_items)}"
            )
            return all_items

        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)

        except (KeyError, ValueError) as e:
            error_msg = f"Invalid data format from API: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)

        except Exception as e:
            error_msg = f"Unexpected error during extraction: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """
        Extract data from the source.

        Returns:
            List of extracted items
        """
        pass
