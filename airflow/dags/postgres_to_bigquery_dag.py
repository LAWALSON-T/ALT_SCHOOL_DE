import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
from google.cloud.bigquery import SchemaField
import os
from os import getenv
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Define default_args for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize Airflow DAG
dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    description='ETL from Postgres to BigQuery',
    schedule_interval=timedelta(days=1)
)

def get_postgres_engine():
    """Get PostgreSQL engine from environment variables"""
    postgres_host = getenv('POSTGRES_HOST', 'ingestion-postgres')
    postgres_port = getenv('POSTGRES_PORT', '5432')
    postgres_db = getenv('POSTGRES_DB', 'ecommerce')
    postgres_user = getenv('POSTGRES_USER')
    postgres_password = getenv('POSTGRES_PASSWORD')
    
    # Validate required environment variables
    if not postgres_user:
        raise ValueError("POSTGRES_USER environment variable not set")
    if not postgres_password:
        raise ValueError("POSTGRES_PASSWORD environment variable not set")
    
    postgres_conn_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'
    logging.info(f"Connecting to PostgreSQL at {postgres_host}:{postgres_port}/{postgres_db}")
    return create_engine(postgres_conn_string)

def get_bigquery_client():
    """Get BigQuery client using secure credential management"""
    try:
        # Method 1: Try to use Airflow connection first (recommended for production)
        try:
            gcp_conn = BaseHook.get_connection('google_cloud_default')
            
            if gcp_conn.extra_dejson.get('key_path'):
                credentials = service_account.Credentials.from_service_account_file(
                    gcp_conn.extra_dejson['key_path']
                )
                project_id = gcp_conn.extra_dejson.get('project_id')
                logging.info("Using Airflow connection for BigQuery authentication")
                return bigquery.Client(credentials=credentials, project=project_id)
        except Exception as e:
            logging.warning(f"Airflow connection not found or invalid: {e}")
        
        # Method 2: Use environment variables as fallback
        key_filename = getenv('key')
        project_id = getenv('key_id')
        
        if not key_filename or not project_id:
            # Method 3: Try default credentials (works on GCP environments)
            try:
                project_id = getenv('key_id')
                if not project_id:
                    raise ValueError("key_id environment variable not set")
                
                client = bigquery.Client(project=project_id)
                logging.info("Using default Google Cloud credentials")
                return client
            except Exception as e:
                raise ValueError(
                    "BigQuery authentication failed. Please ensure either:\n"
                    "1. Airflow connection 'google_cloud_default' is configured, OR\n"
                    "2. Environment variables 'key' and 'key_id' are set, OR\n"
                    "3. Default Google Cloud credentials are available"
                )
        
        # Use service account file from environment variables
        credentials_path = f"/opt/airflow/data/key/{key_filename}"
        
        # Also check GOOGLE_APPLICATION_CREDENTIALS path as fallback
        if not os.path.exists(credentials_path):
            google_creds_path = getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if google_creds_path and os.path.exists(google_creds_path):
                credentials_path = google_creds_path
                logging.info(f"Using GOOGLE_APPLICATION_CREDENTIALS path: {credentials_path}")
            else:
                raise FileNotFoundError(f"Service account key file not found: {credentials_path}")
        
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=project_id)
        logging.info(f"Using service account file for BigQuery authentication: {credentials_path}")
        logging.info(f"BigQuery client initialized for project: {project_id}")
        return client
        
    except Exception as e:
        logging.error(f"Failed to initialize BigQuery client: {e}")
        raise

def execute_sql_script(file_path):
    """Execute SQL script in BigQuery"""
    try:
        client = get_bigquery_client()
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"SQL script file not found: {file_path}")
        
        with open(file_path, 'r') as sql_file:
            sql_script = sql_file.read()
        
        if not sql_script.strip():
            logging.warning(f"SQL script is empty: {file_path}")
            return
        
        logging.info(f"Executing SQL script: {file_path}")
        job = client.query(sql_script)
        job.result()  # Wait for the job to complete
        logging.info(f"Successfully executed SQL script: {file_path}")
        
    except Exception as e:
        logging.error(f"Failed to execute SQL script {file_path}: {e}")
        raise

def infer_bq_schema_from_df(df):
    """
    Infer BigQuery schema from the DataFrame's dtypes.
    """
    schema = []
    for column, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            field_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            field_type = "FLOAT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "TIMESTAMP"
        elif pd.api.types.is_bool_dtype(dtype):
            field_type = "BOOLEAN"
        else:
            field_type = "STRING"
        
        schema.append(SchemaField(column, field_type))
    return schema

def load_postgres_to_bigquery(**kwargs):
    """Load data from PostgreSQL to BigQuery"""
    engine = None
    client = None
    
    try:
        # Initialize connections
        engine = get_postgres_engine()
        client = get_bigquery_client()
        
        # Get BigQuery dataset from environment or use default
        dataset_name = getenv('BQ_DATASET', 'etl_dataset')
        project_id = getenv('key_id')
        
        if not project_id:
            # Extract from client if not in env
            project_id = client.project
        
        logging.info(f"Loading data to BigQuery project: {project_id}, dataset: {dataset_name}")
        
        # Fetch all table names from the PostgreSQL schema
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        result = engine.execute(query)
        table_names = [row[0] for row in result.fetchall()]
        
        if not table_names:
            logging.warning("No tables found in PostgreSQL database")
            return
        
        logging.info(f"Found {len(table_names)} tables to process: {table_names}")
        
        successful_loads = 0
        failed_loads = 0
        
        # Process each table
        for table in table_names:
            try:
                logging.info(f"Processing table: {table}")
                
                # Read table from PostgreSQL
                df = pd.read_sql_table(table, con=engine)
                
                if df.empty:
                    logging.warning(f"Table {table} is empty, skipping...")
                    continue
                
                logging.info(f"Loaded {len(df)} rows from table {table}")
                logging.debug(f"DataFrame dtypes for {table}: {df.dtypes.to_dict()}")
                
                # Define BigQuery table ID
                table_id = f"{project_id}.{dataset_name}.{table}"
                
                # Infer schema from DataFrame
                schema = infer_bq_schema_from_df(df)
                
                # Check if table exists in BigQuery
                try:
                    existing_table = client.get_table(table_id)
                    logging.info(f"Table {table_id} already exists in BigQuery")
                except bigquery.exceptions.NotFound:
                    logging.info(f"Table {table_id} does not exist, will be created")
                
                # Data type conversions to ensure compatibility
                for column in df.columns:
                    bq_type = next((field.field_type for field in schema if field.name == column), 'STRING')
                    
                    try:
                        if bq_type == 'INTEGER':
                            df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                        elif bq_type == 'FLOAT':
                            df[column] = pd.to_numeric(df[column], errors='coerce')
                        elif bq_type == 'TIMESTAMP':
                            df[column] = pd.to_datetime(df[column], errors='coerce')
                        elif bq_type == 'BOOLEAN':
                            df[column] = df[column].astype('boolean')
                        else:  # STRING
                            df[column] = df[column].astype(str)
                    except Exception as e:
                        logging.warning(f"Failed to convert column {column} to {bq_type}: {e}")
                        # Keep original data type if conversion fails
                
                # Configure load job
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
                )
                
                # Load DataFrame to BigQuery
                logging.info(f"Loading data to BigQuery table: {table_id}")
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()  # Wait for completion
                
                # Verify the load
                loaded_table = client.get_table(table_id)
                logging.info(f"Successfully loaded {loaded_table.num_rows} rows to {table_id}")
                successful_loads += 1
                
            except Exception as e:
                logging.error(f"Failed to load table {table}: {str(e)}")
                failed_loads += 1
                # Continue processing other tables instead of failing completely
                continue
        
        # Summary
        total_tables = len(table_names)
        logging.info(f"Load summary: {successful_loads}/{total_tables} tables loaded successfully, {failed_loads} failed")
        
        if failed_loads > 0 and successful_loads == 0:
            raise Exception("All table loads failed")
        elif failed_loads > 0:
            logging.warning(f"{failed_loads} tables failed to load, but {successful_loads} succeeded")
            
    except Exception as e:
        logging.error(f"Failed to load data from PostgreSQL to BigQuery: {e}")
        raise
    finally:
        # Clean up connections
        if engine:
            engine.dispose()

# Define file paths from environment variables
sql_script_path = getenv('SQL_SCRIPT_PATH', '/opt/airflow/data/sql_script/init1.sql')

# Task definitions
init_task = PythonOperator(
    task_id='execute_sql_script',
    python_callable=execute_sql_script,
    op_kwargs={'file_path': sql_script_path},
    dag=dag
)

load_task = PythonOperator(
    task_id='load_postgres_to_bigquery',
    python_callable=load_postgres_to_bigquery,
    dag=dag
)

# Define task dependencies
init_task >> load_task