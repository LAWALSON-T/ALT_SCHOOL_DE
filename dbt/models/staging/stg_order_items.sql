-- models/staging/stg_order_items.sql

select
    order_id,
    product_id,
    price,
    freight_value,
    order_item_id
from {{ source('etl_dataset_source', 'order_items') }}  -- Pulls data from the 'order_items' table in PostgreSQL
