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
04_xcom.py — Airflow 3.1.x style

Migration from 2.10:
- XCom is JSON-only. Pickle support REMOVED (migration 0049).
  Any non-JSON-serializable value must be converted before pushing.
- BaseXCom renamed to XComModel (internal class; import path unchanged for users).
- xcom_pull execution_date= param removed → use run_id= instead.
- execution_date context variable removed → use logical_date.
- Custom XCom backends must subclass XComModel (not BaseXCom).
- Workers communicate via Execution API — XCom get/set goes through API server,
  not directly to the DB.
"""

from datetime import datetime

from airflow.sdk import DAG, task

# XComModel is the new internal name. For user imports, airflow.models.xcom still works.
# The XCOM_RETURN_KEY constant value ("return_value") is unchanged.
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel


# Custom XCom backend in 3.x: must subclass XComModel (not BaseXCom).
# MUST be JSON-serializable — pickle is gone.
class CustomXComBackend(XComModel):
    @staticmethod
    def serialize_value(value, **kwargs):
        import json

        # JSON-only serialization in 3.x. Pickle is not supported.
        # For non-serializable types, convert to a JSON-friendly format first.
        return json.dumps(value).encode()

    @staticmethod
    def deserialize_value(result):
        import json

        return json.loads(result.value)


with DAG(
    dag_id="xcom_demo_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "xcom"],
) as dag:

    def push_xcom(**context):
        ti = context["ti"]
        # xcom_push: values MUST be JSON-serializable in 3.x.
        ti.xcom_push(key="my_list", value=[1, 2, 3])
        # Converting a set to a list before pushing (set is not JSON-serializable).
        ti.xcom_push(key="my_set_as_list", value=sorted([1, 2, 3]))

    def pull_xcom(**context):
        ti = context["ti"]
        # xcom_pull in 3.x: use run_id= instead of execution_date=.
        run_id = context["run_id"]
        my_list = ti.xcom_pull(task_ids="push_task", key="my_list")
        # run_id param is the 3.x equivalent of execution_date=.
        my_list_by_run = ti.xcom_pull(
            task_ids="push_task",
            key="my_list",
            run_id=run_id,
        )
        print(f"my_list={my_list}, by_run_id={my_list_by_run}")

    from airflow.providers.standard.operators.python import PythonOperator

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_xcom,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_xcom,
    )

    push_task >> pull_task


# TaskFlow DAG — all XCom values must be JSON-serializable.
with DAG(
    dag_id="xcom_taskflow_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "xcom"],
) as dag2:

    @task
    def produce() -> dict:
        # dict is JSON-serializable — works in both 2.x and 3.x.
        return {"key": "value", "count": 42}

    @task
    def produce_converted() -> list:
        # 3.x: convert set to list before returning (sets are not JSON-serializable).
        return sorted([1, 2, 3])  # list instead of set

    @task
    def consume(data: dict) -> None:
        print(f"Received: {data}")

    @task
    def consume_list(data: list) -> None:
        # In 3.x, work with the list representation.
        original_set = set(data)  # reconstruct if needed
        print(f"Received as list: {data}, as set: {original_set}")

    d = produce()
    lst = produce_converted()
    consume(d)
    consume_list(lst)
