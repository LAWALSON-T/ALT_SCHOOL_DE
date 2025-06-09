-- models/staging/stg_customers.sql

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from {{ source('etl_dataset_source', 'customers') }}  -- Reference to the raw 'customers' table in PostgreSQL
