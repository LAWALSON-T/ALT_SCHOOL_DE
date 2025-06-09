-- Create tables in the correct order with IF NOT EXISTS clause
CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.customers` (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix STRING,
    customer_city STRING,
    customer_state STRING
);

CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.sellers` (
    seller_id STRING,
    seller_zip_code_prefix STRING,
    seller_city STRING,
    seller_state STRING
);

CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.products` (
    product_id STRING,
    product_category_name STRING,
    product_name_length INT64,
    product_description_length INT64,
    product_photos_qty INT64,
    product_weight_g INT64,
    product_length_cm INT64,
    product_height_cm INT64,
    product_width_cm INT64
);

CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.orders` (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.order_items` (
    order_id STRING,
    order_item_id INT64,
    product_id STRING,
    seller_id STRING,
    shipping_limit_date TIMESTAMP,
    price FLOAT64,
    freight_value FLOAT64
);

CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.product_category_name_translation` (
    product_category_name STRING,
    product_category_name_english STRING
);

-- Create geolocation table
CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.geolocation` (
    geolocation_zip_code_prefix STRING,
    geolocation_lat NUMERIC(10, 8),
    geolocation_lng NUMERIC(11, 8),
    geolocation_city STRING,
    geolocation_state STRING
);

-- Create order_reviews table
CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.order_reviews` (
    review_id STRING,
    order_id STRING,
    review_score INT64,
    review_comment_title STRING,
    review_comment_message STRING,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- Create order_payments table
CREATE TABLE IF NOT EXISTS `my-etl-project-434409.etl_dataset.order_payments` (
    order_id STRING,
    payment_sequential INT64,
    payment_type STRING,
    payment_installments INT64,
    payment_value NUMERIC(10, 2)
);
