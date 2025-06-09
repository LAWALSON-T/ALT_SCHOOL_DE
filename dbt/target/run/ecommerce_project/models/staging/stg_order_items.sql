

  create or replace view `my-etl-project-434409`.`etl_dataset`.`stg_order_items`
  OPTIONS()
  as -- models/staging/stg_order_items.sql

select
    order_id,
    product_id,
    price,
    freight_value,
    order_item_id
from `my-etl-project-434409`.`etl_dataset`.`order_items`  -- Pulls data from the 'order_items' table in PostgreSQL;

