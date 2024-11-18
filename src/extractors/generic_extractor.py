from typing import List, Dict, Any
from .base_extractor import BaseExtractor


class GenericExtractor(BaseExtractor):
    def __init__(self, base_url: str, endpoint: str):
        super().__init__(base_url, endpoint)

    def extract(self) -> List[Dict[str, Any]]:
        return self.fetch_all_pages()
