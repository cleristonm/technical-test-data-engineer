import os
import sys
from pathlib import Path

dag_path = Path(__file__).parent.parent
sys.path.append(str(dag_path))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from src.extractors.generic_extractor import GenericExtractor
from src.transformers.tracks_transformer import TracksTransformer
from src.transformers.users_transformer import UsersTransformer
from src.transformers.listen_history_transformer import ListenHistoryTransformer
from src.loaders.generic_postgres_loader import GenericPostgresLoader
from src.loaders.listen_history_postgres_loader import ListenHistoryPostgresLoader
from ast import literal_eval

logger = LoggingMixin().log

def extract_data(entity_name, extractor):
    logger.info(f"Starting extraction for {entity_name}")
    return extractor.extract()

def transform_data(entity_name, transformer, raw_data):
    logger.info(f"Starting transformation for {entity_name}")
    # Convert string representation to Python object if needed 
    # because XCom returns string representation of the object
    return transformer.transform(literal_eval(raw_data))

def load_data(entity_name, loader, transformed_data):
    logger.info(f"Starting loading for {entity_name}")
    loader.load(entity_name, literal_eval(transformed_data))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('music_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Configure your components
    base_url = 'http://airflow:8000'
    db_params = {
        'dbname': 'music',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }
    
    logger = LoggingMixin().log
    logger.info("Starting DAG")

    # Create tasks for each entity
    entities = ['tracks', 'users', 'listen_history']  
    tasks_by_entity = {}  # Dictionary to store tasks for each entity
    
    for entity in entities:
        # Convert snake_case to PascalCase
        class_name = ''.join(word.title() for word in entity.split('_'))
        transformer_class = globals()[f'{class_name}Transformer']
        
        # Extract task
        extract_task = PythonOperator(
            task_id=f'extract_{entity}',
            python_callable=extract_data,
            op_kwargs={
                'entity_name': entity,
                'extractor': GenericExtractor(base_url, entity)
            }
        )

        # Transform task
        transform_task = PythonOperator(
            task_id=f'transform_{entity}',
            python_callable=transform_data,
            op_kwargs={
                'entity_name': entity,
                'transformer': transformer_class(),
                'raw_data': "{{ task_instance.xcom_pull(task_ids='extract_" + entity + "') }}"
            }
        )

        # Load task
        loader = ListenHistoryPostgresLoader(db_params) if entity == 'listen_history' else GenericPostgresLoader(db_params)
        load_task = PythonOperator(
            task_id=f'load_{entity}',
            python_callable=load_data,
            op_kwargs={
                'entity_name': entity,
                'loader': loader,
                'transformed_data': "{{ task_instance.xcom_pull(task_ids='transform_" + entity + "') }}"
            }
        )

        # Store tasks in dictionary
        tasks_by_entity[entity] = [extract_task, transform_task, load_task]
        
        # Set basic task dependencies within each entity
        extract_task >> transform_task >> load_task

    # Make listen_history wait for tracks and users to complete
    for dependency_entity in ['tracks', 'users']:
        tasks_by_entity[dependency_entity][-1] >> tasks_by_entity['listen_history'][0]