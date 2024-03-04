from datetime import datetime
import json

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator


dag = DAG(
    dag_id="example_http_operator",
    default_args={"retries": 1},
    tags=["example"],
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

dag.doc_md = __doc__

task_post_op = HttpOperator(
    task_id="simple_http_post",
    endpoint="post",
    data=json.dumps({"data": "data"}),
    headers={"Content-Type": "application/json"},
    dag=dag,
)

task_get_op = HttpOperator(
    task_id="get_op",
    method="GET",
    endpoint="get",
    data={"param1": "value1", "param2": "value2"},
    headers={},
    dag=dag,
)

task_post_op >> task_get_op