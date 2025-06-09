-- models/intermediate/int_orders_by_state.sql

with orders_by_state as (
    -- Join orders and customers (or another relevant table) to get state information
    select
        c.customer_state,
        count(o.order_id) as total_orders
    from `my-etl-project-434409`.`etl_dataset`.`stg_orders` o  -- Referencing the 'stg_orders' staging model for orders
    join `my-etl-project-434409`.`etl_dataset`.`stg_customers` c  -- Referencing the 'stg_customers' staging model for customer details
    on o.customer_id = c.customer_id  -- Joining orders and customers by customer_id
    group by c.customer_state
)

select * from orders_by_state