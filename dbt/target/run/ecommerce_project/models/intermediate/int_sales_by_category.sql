

  create or replace view `my-etl-project-434409`.`etl_dataset`.`int_sales_by_category`
  OPTIONS()
  as -- models/intermediate/int_sales_by_category.sql

with category_sales as (
    select
        p.product_category_name,
        sum(oi.price) as total_sales
    from `my-etl-project-434409`.`etl_dataset`.`stg_products` p  -- Referencing the staging model 'stg_products'
    join `my-etl-project-434409`.`etl_dataset`.`stg_order_items` oi  -- Referencing the staging model 'stg_orders_items'
    on p.product_id = oi.product_id  -- Joining on product_id
    group by p.product_category_name
)

select * from category_sales;

