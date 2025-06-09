import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from datetime import datetime, timedelta
import os
from os import getenv
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    filename='/opt/airflow/logs/ingestion_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create SQLAlchemy engine using correct environment variable names
engine = create_engine(f'postgresql://{getenv("POSTGRES_USER")}:{getenv("POSTGRES_PASSWORD")}@{getenv("POSTGRES_HOST")}:{getenv("POSTGRES_PORT")}/{getenv("POSTGRES_DB")}')

# Function to validate order_id in order_items
def validate_order_ids(df, connection):
    order_ids = df['order_id'].unique().tolist()

    if not order_ids:
        logging.warning(f"No order_ids found in the DataFrame.")
        return df

    quoted_order_ids = ", ".join(f"'{order_id}'" for order_id in order_ids)

    query = f"SELECT order_id FROM orders WHERE order_id IN ({quoted_order_ids})"
    try:
        existing_order_ids = pd.read_sql(query, connection)
        valid_ids = set(existing_order_ids['order_id'])

        df_valid = df[df['order_id'].isin(valid_ids)]
        missing_order_ids = set(order_ids) - valid_ids
        if missing_order_ids:
            logging.warning(f"Missing order_ids in orders table: {missing_order_ids}")

        if df_valid.empty:
            logging.warning(f"All order_ids in the file are invalid. No rows will be ingested.")
        return df_valid

    except Exception as e:
        logging.error(f"Error validating order_ids: {e}")
        raise

# Function to validate product_id in order_items
def validate_product_ids(df, connection):
    product_ids = df['product_id'].unique().tolist()

    if not product_ids:
        logging.warning(f"No product_ids found in the DataFrame.")
        return df

    quoted_product_ids = ", ".join(f"'{product_id}'" for product_id in product_ids)

    query = f"SELECT product_id FROM products WHERE product_id IN ({quoted_product_ids})"
    try:
        existing_product_ids = pd.read_sql(query, connection)
        valid_ids = set(existing_product_ids['product_id'])

        df_valid = df[df['product_id'].isin(valid_ids)]
        missing_product_ids = set(product_ids) - valid_ids
        if missing_product_ids:
            logging.warning(f"Missing product_ids in products table: {missing_product_ids}")

        if df_valid.empty:
            logging.warning(f"All product_ids in the file are invalid. No rows will be ingested.")
        return df_valid

    except Exception as e:
        logging.error(f"Error validating product_ids: {e}")
        raise

# Function to validate seller_id in order_items
def validate_seller_ids(df, connection):
    seller_ids = df['seller_id'].unique().tolist()

    if not seller_ids:
        logging.warning(f"No seller_ids found in the DataFrame.")
        return df

    quoted_seller_ids = ", ".join(f"'{seller_id}'" for seller_id in seller_ids)

    query = f"SELECT seller_id FROM sellers WHERE seller_id IN ({quoted_seller_ids})"
    try:
        existing_seller_ids = pd.read_sql(query, connection)
        valid_ids = set(existing_seller_ids['seller_id'])

        df_valid = df[df['seller_id'].isin(valid_ids)]
        missing_seller_ids = set(seller_ids) - valid_ids
        if missing_seller_ids:
            logging.warning(f"Missing seller_ids in sellers table: {missing_seller_ids}")

        if df_valid.empty:
            logging.warning(f"All seller_ids in the file are invalid. No rows will be ingested.")
        return df_valid

    except Exception as e:
        logging.error(f"Error validating seller_ids: {e}")
        raise

# Function to ingest data from CSV to PostgreSQL with retry logic and foreign key validation
def ingest_csv_to_postgres(csv_file, table_name, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            logging.info(f"Starting ingestion for table {table_name}, attempt {attempt + 1}/{retries}")
            file_path = os.path.join("/opt/airflow/data", csv_file)

            if os.path.exists(file_path):
                df = pd.read_csv(file_path)

                if table_name == 'products':
                    df.rename(columns={"product_name_lenght": "product_name_length",
                                       "product_description_lenght": "product_description_length"}, inplace=True)

                with engine.begin() as connection:
                    if table_name == 'order_items':
                        df = validate_product_ids(df, connection)
                        df = validate_seller_ids(df, connection)
                        df = validate_order_ids(df, connection)

                    connection.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))

                    if not df.empty:
                        df.to_sql(table_name, connection, if_exists='append', index=False, chunksize=500)
                        row_count = len(df)
                        logging.info(f"Successfully ingested {row_count} rows from {csv_file} into {table_name}")
                    else:
                        logging.warning(f"No data to ingest for {table_name} after validation.")

                return  # Exit if successful
            else:
                logging.error(f"File not found: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")

        except OperationalError as e:
            if 'deadlock detected' in str(e):
                logging.warning(f"Deadlock detected during ingestion of {table_name}, attempt {attempt + 1}/{retries}. Retrying after {delay} seconds.")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logging.error(f"Error while ingesting {csv_file} into {table_name}: {e}")
                raise

        except IntegrityError as e:
            if 'ForeignKeyViolation' in str(e):
                logging.error(f"ForeignKeyViolation: {e}")
                raise
            else:
                logging.error(f"IntegrityError while ingesting {csv_file} into {table_name}: {e}")
                raise

        except Exception as e:
            logging.error(f"Error while ingesting {csv_file} into {table_name}: {e}")
            raise

        attempt += 1

    logging.error(f"Failed to ingest {csv_file} into {table_name} after {retries} attempts")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 9),  # Updated to current date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'ingest_ecommerce_data',
    default_args=default_args,
    description='Ingest e-commerce data from CSV to PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,  # Added to prevent backfill of old dates
)

# Create PythonOperators for each table
ingest_customers = PythonOperator(
    task_id='ingest_customers',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'customers.csv', 'table_name': 'customers'},
    dag=dag,
)

ingest_products = PythonOperator(
    task_id='ingest_products',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'products.csv', 'table_name': 'products'},
    dag=dag,
)

ingest_sellers = PythonOperator(
    task_id='ingest_sellers',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'sellers.csv', 'table_name': 'sellers'},
    dag=dag,
)

ingest_orders = PythonOperator(
    task_id='ingest_orders',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'orders.csv', 'table_name': 'orders'},
    dag=dag,
)

ingest_order_items = PythonOperator(
    task_id='ingest_order_items',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'order_items.csv', 'table_name': 'order_items'},
    dag=dag,
)

ingest_product_category = PythonOperator(
    task_id='ingest_product_category_name_translation',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'product_category_name_translation.csv', 'table_name': 'product_category_name_translation'},
    dag=dag,
)

ingest_geolocation = PythonOperator(
    task_id='ingest_geolocation',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'geolocation.csv', 'table_name': 'geolocation'},
    dag=dag,
)

ingest_order_reviews = PythonOperator(
    task_id='ingest_order_reviews',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'order_reviews.csv', 'table_name': 'order_reviews'},
    dag=dag,
)

ingest_order_payments = PythonOperator(
    task_id='ingest_order_payments',
    python_callable=ingest_csv_to_postgres,
    op_kwargs={'csv_file': 'order_payments.csv', 'table_name': 'order_payments'},
    dag=dag,
)

# Set up dependencies to ensure correct order of execution
ingest_customers >> ingest_products >> ingest_sellers >> ingest_orders >> ingest_order_items 

# Ensure other tables run after the core tables
ingest_order_items >> ingest_product_category
ingest_order_items >> ingest_geolocation
ingest_order_items >> ingest_order_reviews
ingest_order_items >> ingest_order_payments