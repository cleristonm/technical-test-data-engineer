import os
import sys
from pathlib import Path

dag_path = Path(__file__).parent.parent
sys.path.append(str(dag_path))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from src.extractors.tracks_extractor import TracksExtractor
from src.transformers.tracks_transformer import TracksTransformer
from src.loaders.postgres_loader import PostgresLoader
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
    entities = ['tracks']  # Add more entities as needed
    
    for entity in entities:
        extractor_class = globals()[f'{entity.title()}Extractor']
        transformer_class = globals()[f'{entity.title()}Transformer']
        
        # Extract task
        extract_task = PythonOperator(
            task_id=f'extract_{entity}',
            python_callable=extract_data,
            op_kwargs={
                'entity_name': entity,
                'extractor': extractor_class(base_url)
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
        load_task = PythonOperator(
            task_id=f'load_{entity}',
            python_callable=load_data,
            op_kwargs={
                'entity_name': entity,
                'loader': PostgresLoader(db_params),
                'transformed_data': "{{ task_instance.xcom_pull(task_ids='transform_" + entity + "') }}"
            }
        )

        # Set task dependencies
        extract_task >> transform_task >> load_task