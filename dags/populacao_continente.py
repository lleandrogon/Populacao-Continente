from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime
import pendulum

from src.africa.extract import *
from src.africa.transform import *
from src.africa.load import *
from src.asia.extract import *
from src.asia.transform import *
from src.caribbean.extract import *
from src.caribbean.transform import *
from src.europe.extract import *
from src.europe.transform import *
from src.north_america.extract import *
from src.north_america.transform import *
from src.oceania.extract import *
from src.oceania.transform import *
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

e_africa = PythonOperator(
    task_id = "extract_africa",
    python_callable = extract_africa,
    dag = dag
)

t_africa = PythonOperator(
    task_id = "transform_africa",
    python_callable = transform_africa,
    dag = dag
)

l_africa = PythonOperator(
    task_id = "load_africa",
    python_callable = load_africa,
    dag = dag
)

e_asia = PythonOperator(
    task_id = "extract_asia",
    python_callable = extract_asia,
    dag = dag
)

t_asia = PythonOperator(
    task_id = "transform_asia",
    python_callable = transform_asia,
    dag = dag
)

e_caribbean = PythonOperator(
    task_id = "extract_caribbean",
    python_callable = extract_caribbean,
    dag = dag
)

t_caribbean = PythonOperator(
    task_id = "transform_caribbean",
    python_callable = transform_caribbean,
    dag = dag
)

e_europe = PythonOperator(
    task_id = "extract_europe",
    python_callable = extract_europe,
    dag = dag
)

t_europe = PythonOperator(
    task_id = "transform_europe",
    python_callable = transform_europe,
    dag = dag
)

e_north_america = PythonOperator(
    task_id = "extract_north_america",
    python_callable = extract_north_america,
    dag = dag
)

t_north_america = PythonOperator(
    task_id = "transform_north_america",
    python_callable = transform_north_america,
    dag = dag
)

e_oceania = PythonOperator(
    task_id = "extract_oceania",
    python_callable = extract_oceania,
    dag = dag
)

t_oceania = PythonOperator(
    task_id = "transform_oceania",
    python_callable = transform_oceania,
    dag = dag
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

ct = SQLExecuteQueryOperator(
    task_id = "create_table",
    conn_id = "continentes",
    sql = "src/sql/create_table.sql",
    dag = dag
)

e_africa >> t_africa

e_asia >> t_asia

e_caribbean >> t_caribbean

e_europe >> t_europe

e_north_america >> t_north_america

e_oceania >> t_oceania

e_south_america >> t_south_america

[
    t_africa,
    t_asia,
    t_caribbean,
    t_europe,
    t_north_america,
    t_oceania,
    t_south_america
] >> ct >> [
    l_africa
]