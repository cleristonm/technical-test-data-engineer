from abc import ABC, abstractmethod
from typing import List, Dict, Any

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform the extracted data"""
        pass 