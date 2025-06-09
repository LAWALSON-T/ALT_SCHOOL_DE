import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account

# PostgreSQL connection setup
postgres_conn_string = 'postgresql://username:password@localhost:5432/ecommerce'
engine = create_engine(postgres_conn_string)

# BigQuery connection setup
credentials = service_account.Credentials.from_service_account_file('"C:\Users\Lawalson\my_etl_project\my-etl-project-434409-3e70c40f6d3a.json".json')
client = bigquery.Client(credentials=credentials, project='your_gcp_project_id')

# Initialize an empty dictionary to store dataframes
dataframes_dict = {}

# Fetch all table names from the PostgreSQL schema
table_names = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';").fetchall()
table_names = [name[0] for name in table_names]

# Loop through the tables and convert them to dataframes
for table in table_names:
    # Convert table to DataFrame
    df = pd.read_sql_table(table, con=engine)
    
    # Store DataFrame in the dictionary
    dataframes_dict[table] = df
    
    # Load DataFrame to BigQuery
    table_id = f"your_dataset_name.{table}"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for the job to complete

    print(f"Loaded {table} to BigQuery")

# Close the PostgreSQL connection
engine.dispose()
