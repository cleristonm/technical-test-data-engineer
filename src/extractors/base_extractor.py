import requests
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from airflow.utils.log.logging_mixin import LoggingMixin

class BaseExtractor(ABC, LoggingMixin):
    def __init__(self, base_url: str):
        self.base_url = base_url
        super().__init__()

    def fetch_all_pages(self, endpoint: str, page_size: int = 100) -> List[Dict]:
        """Fetches all pages from the API"""
        all_items = []
        current_page = 1
        
        self.log.info(f"Starting data extraction from endpoint: {endpoint}")
        
        try:
            while True:
                url = f"{self.base_url}/{endpoint}?page={current_page}&size={page_size}"
                self.log.debug(f"Fetching page {current_page} from URL: {url}")
                
                response = requests.get(url)
                response.raise_for_status()  # Raise exception for bad status codes
                
                data = response.json()
                self.log.debug(f"Received data for page {current_page}")
                
                # Garantir que items é uma lista de dicionários
                items = data['items']
                if isinstance(items, str):
                    self.log.warning(f"Items received as string, converting to object: {items[:100]}...")
                    items = json.loads(items)
                
                items_count = len(items)
                all_items.extend(items)
                self.log.info(f"Added {items_count} items from page {current_page}")
                
                if current_page >= data['pages']:
                    self.log.info(f"Reached last page ({current_page})")
                    break
                    
                current_page += 1
                
            total_items = len(all_items)
            self.log.info(f"Extraction completed. Total items fetched: {total_items}")
            return all_items
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)
            
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            error_msg = f"Invalid data format from API: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error during extraction: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Extract data from the source"""
        pass