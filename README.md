# ETL PROJECT

## Objectives

- Download data from kaggle
- Create docker containers for airflow, DBT , POSTGRES
- Ingest data from local storage to PostgreSQL database
- Load the transformed data into Google BigQuery.
- Transform the data using dbt and prepare it for analytics.
- Analyze the data to answer specific business questions.

## Technology Stack

- Docker: To containerize and orchestrate the Airflow and dbt services.
- Airflow: For orchestrating the ETL processes.
- PostgreSQL: The source database for raw eCommerce data.
- Google BigQuery: The destination where transformed data is loaded and stored.
- dbt (Data Build Tool): To handle the transformations and modeling of the data.
- Python: Used in Airflow for automation scripts.
- GitHub: To manage code and version control.

---

## Steps to Run the Project 

# 🚀 AltSchool Data Engineering ETL Project

This project demonstrates an end-to-end ETL pipeline that extracts data from PostgreSQL and loads it into BigQuery using **Docker**, **Apache Airflow**, and **dbt**.

---

## 📦 Prerequisites

Before running this project, make sure you have the following:

- [Docker](https://docs.docker.com/get-docker/) installed on your machine
- A [Google Cloud Platform (GCP)](https://cloud.google.com/) project with BigQuery enabled
- A **service account** with BigQuery access and a downloaded JSON key file

---

## 🛠️ Quick Setup

### 1. Clone and Configure

```bash
git clone https://github.com/LAWALSON-T/alt_school_DE.git
cd alt_school_DE
cp .env.example .env
```

### 2. Set Up Credentials

#### Option A: Service Account File (Recommended for Local Development)

1. Download your GCP service account key (JSON)
2. Place it in the `./data/key/` directory
3. Update `.env`:

```env
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
GCP_PROJECT_ID=your_project_id
GCP_SERVICE_ACCOUNT_KEY=your-key-file.json
```

#### Option B: Airflow UI Connection (Recommended for Production)

1. Open the Airflow UI → Admin → **Connections**
2. Create a connection with:
   - **ID**: `google_cloud_default`
   - **Type**: `Google Cloud`
   - Fill in project details and key if needed

---

## 🧱 Project Structure

```
alt_school_DE/
├── dags/
│   └── postgres_to_bigquery.py     # Airflow DAG for ETL
├── data/
│   ├── key/                        # GCP service account key (not committed)
│   └── sql_script/
│       └── init1.sql               # PostgreSQL initialization script
├── dbt/                            # DBT models
├── .env                            # Your environment config (not committed)
├── .env.example                    # Template config file
├── docker-compose.yml              # Docker services definition
└── README.md
```

---

## ⚙️ Running the Pipeline

### 1. Build Docker Images

```bash
docker compose build
```

### 2. Start All Services

```bash
docker compose up -d
```

### 3. Trigger ETL Pipeline in Airflow

- Open [http://localhost:8080](http://localhost:8080)
- Login with default creds (`airflow` / `airflow`)
- Locate and **trigger the DAG**: `postgres_to_bigquery`  
  *(If not auto-triggered)*

### 4. Run dbt Transformations

```bash
docker compose run dbt dbt run
```

---

## 🔍 Inspect Results

- Open the **BigQuery console**
- Verify that data was loaded and transformed into your `etl_dataset`

---

## ⚙️ Environment Variables

| Variable                | Description                            | Required | Default                                      |
|------------------------|----------------------------------------|----------|----------------------------------------------|
| `POSTGRES_HOST`        | PostgreSQL host                        | No       | `my_etl_project-ingestion-postgres-1`       |
| `POSTGRES_PORT`        | PostgreSQL port                        | No       | `5432`                                       |
| `POSTGRES_DB`          | PostgreSQL database name               | No       | `ecommerce`                                  |
| `POSTGRES_USER`        | PostgreSQL username                    | Yes      | -                                            |
| `POSTGRES_PASSWORD`    | PostgreSQL password                    | Yes      | -                                            |
| `GCP_PROJECT_ID`       | Google Cloud project ID                | Yes      | -                                            |
| `GCP_SERVICE_ACCOUNT_KEY` | Service account JSON key file name | No*      | -                                            |
| `BQ_DATASET`           | BigQuery dataset name                  | No       | `etl_dataset`                                |
| `SQL_SCRIPT_PATH`      | Path to PostgreSQL init script         | No       | `/opt/airflow/data/sql_script/init1.sql`     |

> * Required unless using Airflow Connection or GCP default credentials.

---

## 🔐 Authentication Options

The pipeline supports three authentication strategies:

1. **Airflow Connection** (`google_cloud_default`)
2. **Service Account File** (from `.env` and `data/key`)
3. **GCP Default Credentials** (for GCP-hosted deployments)

---

## 🧩 Docker DNS Configuration (Windows Fix)

If you run into DNS issues:

- Docker Compose includes this by default:

```yaml
services:
  airflow-webserver:
    dns:
      - 8.8.8.8
      - 8.8.4.4
```

- Also check:
  - Docker Desktop → Settings → Resources → Network
  - Enable: **Use kernel networking for DNS resolution**
  - Restart Docker

---

## 🧰 Troubleshooting

### ❌ DNS Resolution Error

If you get:

```
Failed to resolve 'oauth2.googleapis.com'
```

- Confirm DNS override is active in Docker
- Restart Docker services

### ❌ Authentication Errors

1. Ensure your service account has:
   - **BigQuery Admin** or
   - **BigQuery Data Editor**, **BigQuery Job User**, **BigQuery User**

2. Confirm JSON key is mounted:
```bash
docker exec -it <container> ls /opt/airflow/data/key/
```

3. Test connection:
```bash
docker exec -it <container> python -c "
from google.cloud import bigquery
client = bigquery.Client()
print('Connection successful!')
"
```

### ❌ Corporate Proxy Network

Add these lines to `.env` if needed:

```env
HTTP_PROXY=http://proxy.company.com:8080
HTTPS_PROXY=https://proxy.company.com:8080
```

---

## 📝 Notes

- You can find tokens and credentials in the `.env.example` file.
- The system is designed for flexibility and production-readiness.
- Logs and results can be inspected through Airflow UI and BigQuery Console.

---

## 📬 Contact

For issues or questions, please open an [issue](https://github.com/LAWALSON-T/alt_school_DE/issues) in the GitHub repository.

##  dbt Models and Transformations

1. Raw Models:
   - raw_orders.sql: Extracts data from the PostgreSQL orders table and stores it in BigQuery.
   - raw_order_items.sql: Extracts data from the PostgreSQL order items table and stores it in BigQuery.
   
2. Staging Models:
   - stg_orders.sql: Cleans and standardizes the raw orders data,making it easier to work with in subsequent transformations
   - stg_order_items.sql: Cleans and standardizes the order items data, which includes calculating item totals.
   - stg_products.sql: This staging model organizes data from the products table, making product details easier to join with other models.
   - stg_customers.sql: This staging model organizes data from the customers table, making customers details easier to join with other models.
     
3. Intermediate Models:
   - int_sales_by_category.sql: This intermediate model aggregates the sales data by product category. It joins the stg_orders_items and stg_products (dimension table) to 
     get the total sales for each product category.
   - int_avg_deliver_time.sql: This intermediate model calculates the delivery time for each order and then calculates the average delivery time per customer.
   - int_orders_by_state.sql: This intermediate model joins orders and customers table to get state information and counts orders per state

3. Final Models:
   - fct_sales_by_category.sql: This final model pulls data from the intermediate model int_sales_by_category to create a materialized view that holds the total sales by 
     product category.
   - fct_avg_delivery_time.sql: This final model calculates the overall average delivery time across all customers, using the intermediate model int_avg_delivery_time.
   - fct_orders_by_state.sql: This final model aggregates the total number of orders by customer state.
   
4. dbt Macros and Utils:
   - dbt_utils: A collection of utilities to simplify the development of complex SQL transformations.

---

## Analytical Questions Answered

- The average delivery time for orders across all locations is approximately 12.09 days
  
- Which products are selling the most?
  Beleza _saude is the product category with the most sales followed closely by relogos_presentes

- Sau Paulo with state code SP has the most orders , followed by Rio de Janeiro with code RJ

---



