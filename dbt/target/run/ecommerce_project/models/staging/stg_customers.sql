

  create or replace view `my-etl-project-434409`.`etl_dataset`.`stg_customers`
  OPTIONS()
  as -- models/staging/stg_customers.sql

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from `my-etl-project-434409`.`etl_dataset`.`customers`  -- Reference to the raw 'customers' table in PostgreSQL;

