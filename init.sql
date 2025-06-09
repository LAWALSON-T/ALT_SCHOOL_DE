-- Create role
-- CREATE ROLE IF NOT EXISTS lawalson WITH LOGIN PASSWORD 'lawalson';

-- Create tables in the correct order
CREATE TABLE customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

CREATE TABLE sellers (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE products (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

CREATE TABLE orders (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE order_items (
    order_id VARCHAR,
    order_item_id INTEGER,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR,
    product_category_name_english VARCHAR
);


-- Create geolocation table
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat DECIMAL(10, 8),
    geolocation_lng DECIMAL(11, 8),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2)
);

-- Create order_reviews table
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id VARCHAR(32),
    order_id VARCHAR(32),
    review_score INT,
    review_comment_title VARCHAR(100),
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- Create order_payments table
CREATE TABLE IF NOT EXISTS order_payments (
    order_id VARCHAR(32),
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10, 2)
);


-- Grant privileges
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO lawalson;


