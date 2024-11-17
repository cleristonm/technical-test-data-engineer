from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from .base_postgres_loader import BasePostgresLoader

class GenericPostgresLoader(BasePostgresLoader):
    def __init__(self, connection_params: Dict[str, str]):
        super().__init__()
        self.connection_params = connection_params

    def load(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        if not data:
            self.log.info(f"No data to load for table {table_name}")
            return

        columns = data[0].keys()
        values = [[row[column] for column in columns] for row in data]
        
        self.log.info(f"Starting to load {len(data)} records into table {table_name}")
        
        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cur:
                    insert_query = f"""
                        INSERT INTO {table_name} ({','.join(columns)})
                        VALUES %s
                        ON CONFLICT (id) DO UPDATE
                        SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col != 'id')}
                    """
                    self.log.debug(f"Insert query: {insert_query} \nValues: {values}")
                    execute_values(cur, insert_query, values)
                    self.log.info(f"Successfully loaded {len(data)} records into table {table_name}")
                    
        except Exception as e:
            self.log.error(f"Error loading data into {table_name}: {str(e)}")
            raise 