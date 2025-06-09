with avg_delivery_time as (
    -- Reference the intermediate model to get the average delivery time per customer
    select
        customer_id,
        avg_delivery_days
    from {{ ref('int_avg_delivery_time') }}  -- Referencing the intermediate model 'int_avg_delivery_time'
),

overall_avg_delivery_time as (
    -- Calculate the overall average delivery time across all customers
    select
        avg(avg_delivery_days) as overall_avg_delivery_days
    from avg_delivery_time
)

select
    overall_avg_delivery_days
from overall_avg_delivery_time
