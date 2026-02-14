from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import pendulum

from src.south_america.extract import *
from src.south_america.transform import *

dag = DAG(
    dag_id = "populacao_continente",
    schedule = "0 21 * * *",
    default_args = {
        "owner": "airflow",
        "retries": 1,
        "start_date": pendulum.datetime(2026, 2, 13, tz = "America/Sao_Paulo")
    },
    catchup = False,
    tags = ["populacao", "continente"]
)

e_south_america = PythonOperator(
    task_id = "extract_south_america",
    python_callable = extract_south_america,
    dag = dag
)

t_south_america = PythonOperator(
    task_id = "transform_south_america",
    python_callable = transform_south_america,
    dag = dag
)

e_south_america >> t_south_america