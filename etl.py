import logging
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

# Setup logging
logging.basicConfig(
    filename='/opt/airflow/logs/ingestion_log.log',  # Log file name with absolute path
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

# Database connection details
db_username = 'lawalson'
db_password = 'lawalson'
db_host = 'ingestion-postgres'
db_port = '5432'
db_name = 'ecommerce'

# Create SQLAlchemy engine
engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Function to ingest data from CSV to PostgreSQL
def ingest_csv_to_postgres(csv_file, table_name):
    try:
        logging.info(f"Starting ingestion for table {table_name}")
        
        # Construct the file path
        file_path = os.path.join("/opt/airflow/data", csv_file)
        
        # Check if the file exists
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            
            with engine.begin() as connection:
                # Truncate the table instead of dropping it
                connection.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                
                # Use to_sql with if_exists='append' to insert the data
                df.to_sql(table_name, connection, if_exists='append', index=False)
            
            logging.info(f"Successfully ingested {csv_file} into {table_name}")
        else:
            logging.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
    except Exception as e:
        logging.error(f"Error while ingesting {csv_file} into {table_name}: {e}")
        raise

# Task to list data directory contents
def list_data_directory():
    try:
        data_directory = "/opt/airflow/data"
        contents = os.listdir(data_directory)
        print(f"Contents of {data_directory}: {contents}")
        logging.info(f"Contents of {data_directory}: {contents}")
    except Exception as e:
        logging.error(f"Error listing directory contents: {e}")
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_ecommerce_data',
    default_args=default_args,
    description='Ingest e-commerce data from CSV to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Create tasks for each table
tables = ['customers', 'order_items', 'orders', 'product_category_name_translation', 'products', 'sellers','geolocation','order_reviews','order_payments']

ingest_tasks = []

for table in tables:
    task = PythonOperator(
        task_id=f'ingest_{table}',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'csv_file': f'{table}.csv', 'table_name': table},
        dag=dag,
    )
    ingest_tasks.append(task)

# List data files task
list_files_task = PythonOperator(
    task_id='list_data_files',
    python_callable=list_data_directory,
    dag=dag
)

# Set up dependencies
list_files_task >> ingest_tasks
