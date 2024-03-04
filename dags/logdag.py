import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="logdag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start >> end