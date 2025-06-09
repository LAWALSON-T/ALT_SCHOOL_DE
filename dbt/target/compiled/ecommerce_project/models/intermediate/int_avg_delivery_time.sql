with delivery_times as (
    -- Calculate the delivery time for each order
    select
        order_id,
        customer_id,
        order_purchase_timestamp,
        order_delivered_customer_date,
        date_diff(order_delivered_customer_date, order_purchase_timestamp, DAY) as delivery_days
    from `my-etl-project-434409`.`etl_dataset`.`stg_orders`  -- Referencing the 'stg_orders' staging model
    where order_delivered_customer_date is not null  -- Only consider orders that have been delivered
)

, avg_delivery_time as (
    -- Calculate the average delivery time for all orders
    select
        customer_id,
        avg(delivery_days) as avg_delivery_days
    from delivery_times
    group by customer_id
)

select * from avg_delivery_time