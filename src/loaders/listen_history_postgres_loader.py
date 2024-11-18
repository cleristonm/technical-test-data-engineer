"""
PostgreSQL loader specifically designed for listen history data.

This module provides a specialized implementation for loading listen history data
into PostgreSQL databases. It includes features such as:
- User ID validation
- Duplicate record handling
- Batch processing optimization
- Automatic cleanup of outdated records
"""

from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from .base_postgres_loader import BasePostgresLoader


class ListenHistoryPostgresLoader(BasePostgresLoader):
    """
    Specialized loader for managing listen history records in PostgreSQL.

    This class extends the base PostgreSQL loader to handle the specific requirements
    of listen history data, including:
    - Validating user IDs against the users table
    - Removing existing records before insertion
    - Handling batch insertions efficiently
    - Managing data consistency across related tables

    Attributes:
        connection_params: Dictionary containing database connection parameters
    """

    def __init__(self, connection_params: Dict[str, str]):
        super().__init__()
        self.connection_params = connection_params

    def load(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """
        Load listen history data into PostgreSQL with user validation.

        Args:
            table_name: Name of the target table
            data: List of dictionaries containing listen history records

        Raises:
            psycopg2.Error: If a database error occurs during loading
        """
        if not data:
            self.log.info("No data to load for listen history")
            return

        try:
            with psycopg2.connect(**self.connection_params) as conn:
                with conn.cursor() as cur:
                    # Validate user_ids against users table
                    user_ids = list(set(row["user_id"] for row in data))
                    validation_query = """
                        SELECT id FROM users WHERE id = ANY(%s)
                    """
                    cur.execute(validation_query, (user_ids,))
                    valid_user_ids = {row[0] for row in cur.fetchall()}

                    # Separate valid and invalid records
                    valid_records = []
                    invalid_records = []
                    for record in data:
                        if record["user_id"] in valid_user_ids:
                            valid_records.append(record)
                        else:
                            invalid_records.append(record)

                    if invalid_records:
                        self.log.warning(
                            f"Found {len(invalid_records)} records with non-existent user_ids:"
                        )
                        for record in invalid_records:
                            self.log.warning(
                                f"Skipping record: "
                                f"user_id={record['user_id']}, "
                                f"track_id={record['track_id']}, "
                                f"updated_at={record['updated_at']}"
                            )

                    if not valid_records:
                        self.log.info("No valid records to load")
                        return

                    columns = valid_records[0].keys()
                    values = [
                        [row[column] for column in columns] for row in valid_records
                    ]

                    self.log.info(
                        f"Starting to load {len(valid_records)} valid listen history records"
                    )

                    # Delete existing records that might conflict
                    delete_query = """
                        DELETE FROM listen_history 
                        WHERE (user_id, track_id, updated_at) IN (
                            SELECT user_id, track_id, updated_at 
                            FROM unnest(%s::int[], %s::int[], %s::timestamp[])
                            AS t(user_id, track_id, updated_at)
                        )
                    """
                    user_ids = [row["user_id"] for row in valid_records]
                    track_ids = [row["track_id"] for row in valid_records]
                    updated_ats = [row["updated_at"] for row in valid_records]

                    cur.execute(delete_query, (user_ids, track_ids, updated_ats))

                    # Insert new records
                    insert_query = f"""
                        INSERT INTO {table_name} ({','.join(columns)})
                        VALUES %s
                    """
                    self.log.debug(f"Insert query: {insert_query}")
                    execute_values(cur, insert_query, values)
                    self.log.info(
                        f"Successfully loaded {len(valid_records)} listen history records"
                    )
                    self.log.info(f"Skipped {len(invalid_records)} invalid records")

        except Exception as e:
            self.log.error(f"Error loading listen history data: {str(e)}")
            raise
