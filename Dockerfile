# docker build -t airflow-extended:latest -f Dockerfile .
FROM apache/airflow:2.4.2-python3.10

# this fixed issues with log volume
USER root
RUN chmod 777 -R /opt/airflow/

USER airflow

# generate requirements file via poetry
COPY requirements.txt /opt/airflow/
# Dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
