# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
04_xcom.py — Airflow 2.10.x style

Demonstrates:
- Explicit xcom_push / xcom_pull
- TaskFlow implicit XCom via return values
- Pickle-based XCom backend (supported in 2.x, REMOVED in 3.x)
- BaseXCom class name (renamed to XComModel in 3.x internals)
- xcom_push/xcom_pull with execution_date param (replaced by run_id in 3.x)
- Storing arbitrary Python objects in XCom (works in 2.x with pickle,
  breaks in 3.x which is JSON-only)
"""

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

# BaseXCom is the XCom backend base class in 2.x.
# RENAMED to XComModel in 3.x.
from airflow.models.xcom import BaseXCom, XCOM_RETURN_KEY


# Example of a custom XCom backend (2.x style).
# In 2.x you can override serialize_value to store pickled Python objects.
class CustomXComBackend(BaseXCom):
    @staticmethod
    def serialize_value(value, **kwargs):
        import pickle

        # Pickle serialization — SUPPORTED in 2.x, REMOVED in 3.x.
        # In 3.x XCom is JSON-only; pickle support was removed (migration 0049).
        return pickle.dumps(value)

    @staticmethod
    def deserialize_value(result):
        import pickle

        return pickle.loads(result.value)


with DAG(
    dag_id="xcom_demo_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "xcom"],
) as dag:

    def push_xcom(**context):
        ti = context["ti"]
        # Explicit xcom_push: in 2.x you can push any picklable Python object.
        ti.xcom_push(key="my_list", value=[1, 2, 3])
        # Pushing a non-JSON-serializable object works in 2.x (pickle backend).
        # This would FAIL in 3.x which is JSON-only.
        ti.xcom_push(key="my_set", value={1, 2, 3})  # set is not JSON serializable

    def pull_xcom(**context):
        ti = context["ti"]
        # xcom_pull in 2.x can use execution_date to find the XCom.
        # execution_date parameter was replaced by run_id in 3.x.
        my_list = ti.xcom_pull(task_ids="push_task", key="my_list")
        # Pulling with execution_date explicitly (2.x style).
        execution_date = context["execution_date"]
        my_list_by_date = ti.xcom_pull(
            task_ids="push_task",
            key="my_list",
            # execution_date param REMOVED in 3.x — use run_id instead.
            execution_date=execution_date,
        )
        print(f"my_list={my_list}, by_date={my_list_by_date}")

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_xcom,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_xcom,
    )

    push_task >> pull_task


# TaskFlow DAG demonstrating implicit XCom (same syntax in 2.x and 3.x,
# but object types must be JSON-serializable in 3.x).
with DAG(
    dag_id="xcom_taskflow_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "xcom"],
) as dag2:

    @task
    def produce() -> dict:
        # Returning a dict — this is JSON-serializable, works in both 2.x and 3.x.
        return {"key": "value", "count": 42}

    @task
    def produce_non_json() -> set:
        # Returning a set — NOT JSON-serializable.
        # Works in 2.x (serialized as pickle), FAILS in 3.x.
        return {1, 2, 3}

    @task
    def consume(data: dict) -> None:
        print(f"Received: {data}")

    @task
    def consume_set(data: set) -> None:
        print(f"Received set: {data}")

    d = produce()
    s = produce_non_json()
    consume(d)
    consume_set(s)