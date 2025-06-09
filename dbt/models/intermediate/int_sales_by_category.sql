-- models/intermediate/int_sales_by_category.sql

with category_sales as (
    select
        p.product_category_name,
        sum(oi.price) as total_sales
    from {{ ref('stg_products') }} p  -- Referencing the staging model 'stg_products'
    join {{ ref('stg_order_items') }} oi  -- Referencing the staging model 'stg_orders_items'
    on p.product_id = oi.product_id  -- Joining on product_id
    group by p.product_category_name
)

select * from category_sales
