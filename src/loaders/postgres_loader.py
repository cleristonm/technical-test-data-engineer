from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values

class PostgresLoader:
    def __init__(self, connection_params: Dict[str, str]):
        self.connection_params = connection_params

    def load(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        if not data:
            return

        columns = data[0].keys()
        values = [[row[column] for column in columns] for row in data]
        
        with psycopg2.connect(**self.connection_params) as conn:
            with conn.cursor() as cur:
                insert_query = f"""
                    INSERT INTO {table_name} ({','.join(columns)})
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE
                    SET {','.join(f"{col}=EXCLUDED.{col}" for col in columns if col != 'id')}
                """
                execute_values(cur, insert_query, values) 