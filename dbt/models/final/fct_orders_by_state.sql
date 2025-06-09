-- models/final/fct_orders_by_state.sql

with orders_by_state as (
    -- Reference the intermediate model to get the total orders per state
    select
        customer_state,
        total_orders
    from {{ ref('int_orders_by_state') }}  -- Referencing the intermediate model 'int_orders_by_state'
)

select
    customer_state,
    total_orders
from orders_by_state
order by total_orders desc;  -- Order the results by the total number of orders, highest to lowest
