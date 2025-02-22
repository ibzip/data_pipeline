# Use the official Airflow image with multi-platform support
FROM --platform=${BUILDPLATFORM:-linux/amd64} apache/airflow:2.5.1

# Set working directory inside the container
WORKDIR /opt/airflow

# Set build-time platform argument
ARG TARGETPLATFORM
ARG BUILDPLATFORM

# Copy files with explicit permissions
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

# Set the PYTHONPATH to include /opt/airflow/src
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"

# Install additional dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Set correct permissions
RUN chmod -R 777 /opt/airflow/logs /opt/airflow/dags /opt/airflow/src /opt/airflow/sql

# Switch back to airflow user
USER airflow