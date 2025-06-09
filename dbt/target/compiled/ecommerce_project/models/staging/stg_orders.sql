-- models/staging/stg_orders.sql
with orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from `my-etl-project-434409`.`etl_dataset`.`raw_orders`  -- This references the raw_orders model
)

select * from orders