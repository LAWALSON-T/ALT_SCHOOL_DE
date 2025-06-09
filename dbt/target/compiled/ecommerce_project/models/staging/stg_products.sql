-- models/staging/stg_products.sql

with products as (
    -- Select the raw product data from the source table in PostgreSQL
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from `my-etl-project-434409`.`etl_dataset`.`products`  -- Referencing the raw 'products' table
)

, clean_products as (
    -- Clean the raw product data (e.g., handling nulls, renaming columns)
    select
        product_id,
        product_category_name,
        coalesce(product_name_length, 0) as product_name_length,  -- Replace nulls with 0
        coalesce(product_description_length, 0) as product_description_length,  -- Replace nulls with 0
        coalesce(product_photos_qty, 0) as product_photos_qty,  -- Replace nulls with 0
        coalesce(product_weight_g, 0) as product_weight_g,  -- Replace nulls with 0
        coalesce(product_length_cm, 0) as product_length_cm,  -- Replace nulls with 0
        coalesce(product_height_cm, 0) as product_height_cm,  -- Replace nulls with 0
        coalesce(product_width_cm, 0) as product_width_cm  -- Replace nulls with 0
    from products
)

select * from clean_products