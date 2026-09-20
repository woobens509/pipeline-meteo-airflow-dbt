FROM apache/airflow:3.3.1

USER airflow

RUN pip install --no-cache-dir \
    dbt-postgres==1.11.0 \
    requests==2.34.2 \
    psycopg2-binary==2.9.13