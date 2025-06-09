-- models/final/fct_sales_by_category.sql

select
    product_category_name,
    total_sales
from {{ ref('int_sales_by_category') }}