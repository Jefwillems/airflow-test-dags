from datetime import datetime
import json

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task, dag


@dag(
    dag_id="example_http_operator",
    default_args={"retries": 1},
    tags=["example"],
    start_date=datetime(2021, 1, 1),
    catchup=False,
    doc_md=__doc__,
)
def example_http_operator():
    @task()
    def get_time():
        return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    t = get_time()

    task_post_op = HttpOperator(
        http_conn_id="httpbin_conn",
        task_id="simple_http_post",
        endpoint="post",
        data=json.dumps({"data": t}),
        headers={"Content-Type": "application/json"},
    )

    task_get_op = HttpOperator(
        http_conn_id="httpbin_conn",
        task_id="get_op",
        method="GET",
        endpoint="get",
        data={"param1": "value1", "param2": "value2"},
        headers={},
    )

    task_post_op >> task_get_op
