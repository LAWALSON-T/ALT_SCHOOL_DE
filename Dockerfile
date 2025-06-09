# Dockerfile for dbt service
FROM python:3.11-slim

# Set working directory
WORKDIR /usr/app/dbt

# Install dbt-core and plugins
RUN pip install dbt-core==1.8.6 dbt-bigquery==1.8.2 dbt-postgres==1.8.2 dbt-redshift==1.8.1 dbt-snowflake==1.8.3

# Copy dbt project files
COPY ./dbt /usr/app/dbt

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/data/key/my-etl-project-434409-3e70c40f6d3a.json

# Default command to run
CMD ["echo","Hello world!"]