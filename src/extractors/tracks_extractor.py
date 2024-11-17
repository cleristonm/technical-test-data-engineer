from .base_extractor import BaseExtractor
from typing import List, Dict, Any

class TracksExtractor(BaseExtractor):
    def __init__(self, base_url: str):
        super().__init__(base_url)
        
    def extract(self) -> List[Dict[str, Any]]:
        return self.fetch_all_pages('tracks') 