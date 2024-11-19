import os
import sys
from pathlib import Path
from config.database import DB_CONFIG

dag_path = Path(__file__).parent.parent
sys.path.append(str(dag_path))

from datetime import datetime, timedelta
from ast import literal_eval

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from src.extractors.generic_extractor import GenericExtractor
from src.transformers.tracks_transformer import TracksTransformer
from src.transformers.users_transformer import UsersTransformer
from src.transformers.listen_history_transformer import ListenHistoryTransformer
from src.loaders.generic_postgres_loader import GenericPostgresLoader
from src.loaders.listen_history_postgres_loader import ListenHistoryPostgresLoader

# Add project root to path
dag_path = Path(__file__).parent.parent
sys.path.append(str(dag_path))

logger = LoggingMixin().log

# Configuration
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

ENTITIES = ['tracks', 'users', 'listen_history']

BASE_URL = 'http://airflow:8000'

def extract_data(entity_name: str, extractor) -> list:
    """
    Extract data for given entity.
    
    Args:
        entity_name: Name of the entity to extract
        extractor: Extractor instance to use
        
    Returns:
        List of extracted records
    """
    logger.info(f"Starting extraction for {entity_name}")
    return extractor.extract()

def transform_data(entity_name: str, transformer, raw_data: str) -> list:
    """
    Transform raw data for given entity.
    
    Args:
        entity_name: Name of the entity to transform
        transformer: Transformer instance to use
        raw_data: Raw data from XCom
        
    Returns:
        List of transformed records
    """
    logger.info(f"Starting transformation for {entity_name}")
    return transformer.transform(literal_eval(raw_data))

def load_data(entity_name: str, loader, transformed_data: str) -> None:
    """
    Load transformed data for given entity.
    
    Args:
        entity_name: Name of the entity to load
        loader: Loader instance to use
        transformed_data: Transformed data from XCom
    """
    logger.info(f"Starting loading for {entity_name}")
    loader.load(entity_name, literal_eval(transformed_data))

with DAG('music_etl',
         default_args=DEFAULT_ARGS,
         schedule_interval='@daily',
         catchup=False) as dag:

    logger = LoggingMixin().log
    logger.info("Starting DAG")

    tasks_by_entity = {}
    
    for entity in ENTITIES:
        # Get transformer class name
        transformer_class = globals()[f'{"".join(word.title() for word in entity.split("_"))}Transformer']
        
        # Create tasks
        extract_task = PythonOperator(
            task_id=f'extract_{entity}',
            python_callable=extract_data,
            op_kwargs={
                'entity_name': entity,
                'extractor': GenericExtractor(BASE_URL, entity)
            }
        )

        transform_task = PythonOperator(
            task_id=f'transform_{entity}',
            python_callable=transform_data,
            op_kwargs={
                'entity_name': entity,
                'transformer': transformer_class(),
                'raw_data': f"{{{{ task_instance.xcom_pull(task_ids='extract_{entity}') }}}}"
            }
        )

        loader = (ListenHistoryPostgresLoader(DB_CONFIG) if entity == 'listen_history' 
                 else GenericPostgresLoader(DB_CONFIG))
        
        load_task = PythonOperator(
            task_id=f'load_{entity}',
            python_callable=load_data,
            op_kwargs={
                'entity_name': entity,
                'loader': loader,
                'transformed_data': f"{{{{ task_instance.xcom_pull(task_ids='transform_{entity}') }}}}"
            }
        )

        # Store and set dependencies
        tasks_by_entity[entity] = [extract_task, transform_task, load_task]
        extract_task >> transform_task >> load_task

    # Set dependencies for listen_history
    for dependency_entity in ['tracks', 'users']:
        tasks_by_entity[dependency_entity][-1] >> tasks_by_entity['listen_history'][0]