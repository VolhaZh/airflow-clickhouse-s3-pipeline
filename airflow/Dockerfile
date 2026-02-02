FROM apache/airflow:slim-2.10.4-python3.9

USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      libpq-dev \
      curl \
      build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements-airflow.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set the PYTHONPATH to include our project modules
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

WORKDIR ${AIRFLOW_HOME} 