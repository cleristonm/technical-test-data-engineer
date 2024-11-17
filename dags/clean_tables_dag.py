from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable
from datetime import datetime
import psycopg2

logger = LoggingMixin().log

# Initialize variables if they don't exist
if Variable.get('clean_tables_confirmation', default_var=None) is None:
    Variable.set('clean_tables_confirmation', 'no')
if Variable.get('clean_tables_reason', default_var=None) is None:
    Variable.set('clean_tables_reason', '')

def get_confirmation(**context):
    """Check if confirmation and reason are set to proceed with cleanup"""
    confirmation = Variable.get('clean_tables_confirmation', default_var=None)
    reason = Variable.get('clean_tables_reason', default_var=None)
    
    # Get user who triggered the DAG
    user = context['dag_run'].conf.get('user', 'Airflow System') if context['dag_run'].conf else 'Airflow System'
    
    if not all([confirmation, reason]):
        logger.error("Missing required information. Please set cleanup reason.")
        return 'skip_cleanup'
    
    # Validate reason length
    if len(reason) < 20:
        logger.error("Cleanup reason must be at least 20 characters long.")
        return 'skip_cleanup'
    
    if confirmation and confirmation.lower() == 'yes':
        # Log the cleanup request
        logger.info(f"Cleanup requested by: {user}")
        logger.info(f"Reason: {reason}")
        
        # Pass information via XCom
        context['task_instance'].xcom_push(key='cleanup_user', value=user)
        context['task_instance'].xcom_push(key='cleanup_reason', value=reason)
        
        # Reset the variables
        Variable.set('clean_tables_confirmation', 'no')
        Variable.set('clean_tables_reason', '')
        
        return 'clean_tables'
    return 'skip_cleanup'

def clean_tables(**context):
    """Clean all tables in the database"""
    # Get information from XCom
    user = context['task_instance'].xcom_pull(task_ids='check_confirmation', key='cleanup_user')
    reason = context['task_instance'].xcom_pull(task_ids='check_confirmation', key='cleanup_reason')
    
    logger.info(f"Starting cleanup process")
    logger.info(f"Requested by: {user}")
    logger.info(f"Reason: {reason}")
    
    db_params = {
        'dbname': 'music',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }
    
    tables = ['listen_history', 'users', 'tracks']
    
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    logger.info(f"Cleaning table: {table}")
                    cur.execute(f"DELETE FROM {table}")
                
        logger.info("All tables cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning tables: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    'clean_all_tables',
    default_args=default_args,
    description='''
    Clean all tables in the database (manual trigger with confirmation)
    
    How to use:
    1. Go to Admin -> Variables
    2. Set the following variables:
       - 'clean_tables_confirmation': 'yes'
       - 'clean_tables_reason': 'Reason for cleanup'
    3. Trigger this DAG
    
    Note: 
    - The user who triggers the DAG will be automatically recorded in logs
    - Variables will automatically reset after checking
    ''',
    schedule_interval=None,
    tags=['cleanup']
) as dag:

    start = EmptyOperator(
        task_id='start'
    )

    check_confirmation = BranchPythonOperator(
        task_id='check_confirmation',
        python_callable=get_confirmation,
    )

    clean_tables = PythonOperator(
        task_id='clean_tables',
        python_callable=clean_tables,
    )

    skip_cleanup = EmptyOperator(
        task_id='skip_cleanup'
    )

    end = EmptyOperator(
        task_id='end',
        trigger_rule='none_failed'
    )

    start >> check_confirmation >> [clean_tables, skip_cleanup] >> end 