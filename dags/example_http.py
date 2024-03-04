from datetime import datetime
import json

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task, dag
import requests

url = "http://catfact.ninja/fact"


@dag(
    dag_id="example_http_dag",
    default_args={"retries": 1},
    tags=["example"],
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=__doc__,
)
def example_http_dag():
    @task()
    def get_time():
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    @task()
    def get_cat_fact(time: str):
        response = requests.get(url)
        return {"cat_fact": response.json()["fact"], time: time}

    @task()
    def print_cat_fact(fact):
        print(f"Cat fact: {fact['cat_fact']} \t Time: {fact['time']}")

    print_cat_fact(get_cat_fact(get_time()))


example_http_dag()
