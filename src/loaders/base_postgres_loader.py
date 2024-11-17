from abc import ABC, abstractmethod
from typing import List, Dict, Any
from airflow.utils.log.logging_mixin import LoggingMixin

class BasePostgresLoader(LoggingMixin, ABC):
    @abstractmethod
    def load(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        pass 