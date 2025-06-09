-- models/final/fct_sales_by_category.sql

select
    product_category_name,
    total_sales
from `my-etl-project-434409`.`etl_dataset`.`int_sales_by_category`