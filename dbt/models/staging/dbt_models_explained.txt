Explanation of the models used to answer the analytical questions.

1. Raw_orders (Staging model for Orders)

Purpose:This model acts as a staging layer for the orders fact table

Source:
- Source Table: orders
- Contains transactional data, such as order_id, customer_id, product_id, order_value, order_status, and timestamps like order_purchase_timestamp and order_delivered_customer_date.

2. stg_orders (Staging and transformation Model for Orders)

Purpose:
- This model acts as a staging layer for the orders fact table. It cleans and organizes the raw data, making it easier to work with in subsequent transformations.

Source:
- Source Table: raw_orders
- Contains transactional data, such as order_id, customer_id, product_id, order_value, order_status, and timestamps like order_purchase_timestamp and order_delivered_customer_date.

Key element:
- It selects columns from the raw orders table and holds it in staging


3. stg_products (Staging Model for Products)

Purpose:
- This staging model organizes data from the products table, making product details easier to join with other models.

Source:
- Source Table: products
- Contains product-related information, including product_id, product_category_name, and product_name.

Key Transformations:
- Renaming Fields: Making field names consistent and replacing null values with zero.

4. stg_customers (Staging Model for Customers)

Purpose:
- This staging model organizes data from the customers table, making customers details easier to join with other models.

Source:
- Source Table: customers
- Contains customer_id,customer_unique_id, customer_city, and customer_state.

Key element:
- It selects columns from the customers table and holds it in staging

5. stg_order_items (Staging Model for orders)

Purpose:
- This staging model organizes data from the order_items table, making order_items details easier to join with other models.

Source:
- Source Table: order_items
- Contains order_id,product_id, price, freight_value, order_item_id.

6. int_sales_by_category (Intermediate Model)

Purpose:
- This intermediate model aggregates the sales data by product category. It joins the stg_orders_items and stg_products (dimension table) to get the total sales for each product category.

Source:
- Source Tables: 
  - stg_orders_items
  - stg_products

Key Transformations:
- Join: Joins the orders_items data with product information based on product_id.
- Aggregation: Sums price by product_category_name to calculate total sales for each category.


7. int_avg_delivery_time (Intermediate Model)

Purpose:
- This intermediate model calculates the delivery time for each order and then calculates the average delivery time per customer.

Source:
- Source Table: stg_orders

Key Transformations:
- Date Difference Calculation: Uses date_diff() to calculate the time between order_purchase_timestamp and order_delivered_customer_date (delivery time).
- Aggregation: Averages the delivery time per customer.


8. int_orders_by_state (Intermediate Model)

Purpose:
- This intermediate model joins orders and customers table to get state information and counts orders per state

Source:
- Source Table: stg_orders, stg_customers

Key Transformations:
- Joins: Joins the orders table with customers table 
- Aggregation: counts order_id.


9. fct_sales_by_category (Final Model)

Purpose:
- This final model pulls data from the intermediate model int_sales_by_category to create a materialized view that holds the total sales by product category.

Source:
- Source Model: int_sales_by_category

Key Transformations:
- Selection: Simply selects the product category and total sales from the intermediate model.


10. fct_avg_delivery_time (Final Model)

Purpose:
- This final model calculates the overall average delivery time across all customers, using the intermediate model int_avg_delivery_time.

Source:
- Source Model: int_avg_delivery_time

Key Transformations:
- Aggregation: Averages the avg_delivery_days across all customers to get the overall average delivery time.



11. fct_orders_by_state (Final Model)

Purpose:
- This final model aggregates the total number of orders by customer state.

Source:
- Source Tables: int_orders_by_state


Key Transformations:
- Aggregation: Counts the number of orders per customer_state and orders it.




Summary:
- Staging Models (stg_orders, stg_products, stg_customers, stg_order_time, raw_orders): These clean and organize raw data, making it easier to work with in downstream models.
- Intermediate Models (int_sales_by_category, int_avg_delivery_time, int_orders_by_state): These models apply transformations like joins and aggregations to prepare the data for final analysis.
- Final Models (fct_sales_by_category, fct_avg_delivery_time, fct_orders_by_state): These models contain the final aggregated data used to answer the analytical questions.

