# Use the official Airflow image
FROM --platform=linux/amd64 apache/airflow:2.5.1

# Set working directory inside the container
WORKDIR /opt/airflow

RUN ls -l
# Copy DAGs from airflow/dags/
COPY airflow/dags/ /opt/airflow/dags/
COPY sql/ /opt/airflow/sql
# Copy the source code from src/
COPY src /opt/airflow/src/

# Copy requirements.txt
COPY requirements.txt /opt/airflow/requirements.txt

# Set the PYTHONPATH to include /opt/airflow/src
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"

# Install additional dependencies if needed
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
