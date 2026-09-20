from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/airflow/projet1/dbt"
EXTRACTION_DIR = "/opt/airflow/projet1/extraction"

ENV_VARS = {
    "METEO_DB_HOST": "postgres_projet1",
    "METEO_DB_PORT": "5432",
    "METEO_DB_NAME": "meteo_db",
    "METEO_DB_USER": "dbt_user",
    "METEO_DB_PASSWORD": "dbt_password",
}

default_args = {
    "owner": "jean_woobens",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline_meteo_projet1",
    description="Extraction météo Open-Meteo + transformation dbt + tests qualité",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["projet1", "meteo"],
) as dag:

    extraction = BashOperator(
        task_id="extraction_meteo",
        bash_command=f"cd {EXTRACTION_DIR} && python extract_weather.py",
        env=ENV_VARS,
        append_env=True,
    )

    transformation_dbt = BashOperator(
        task_id="transformation_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}",
        env=ENV_VARS,
        append_env=True,
    )

    tests_dbt = BashOperator(
        task_id="tests_qualite_dbt",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}",
        env=ENV_VARS,
        append_env=True,
    )

    extraction >> transformation_dbt >> tests_dbt