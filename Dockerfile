#official Airflow image
FROM apache/airflow:2.8.1-python3.10

# Copy requirements
COPY requirements.txt /requirements.txt

# Install dependencies as airflow user (required by Airflow 2.8+)
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Setting directory
WORKDIR /opt/airflow