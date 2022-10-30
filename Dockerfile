# docker build -t airflow-extended:latest -f Dockerfile .
# 2.3.2 is the latest compatible with dbt (due to i.e. jinja conflict)
FROM apache/airflow:2.3.2-python3.10

# this fixed issues with log volume
USER root
RUN chmod 777 -R /opt/airflow/

USER airflow

# Dependencies
COPY requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
