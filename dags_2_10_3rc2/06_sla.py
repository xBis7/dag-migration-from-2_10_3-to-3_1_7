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
06_sla.py — Airflow 2.10.x style

Demonstrates:
- sla_miss_callback on the DAG
- sla= per-task on BaseOperator
- SLA miss records in SlaMiss table (table removed in 3.x)

NOTE: The entire SLA feature was removed in Airflow 3.0.
In 3.1 the replacement is the `deadline=` DAG parameter with DeadlineAlert.
See 06_deadline.py in the 3.x folder.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def my_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Called when an SLA is missed.

    Parameters:
        dag: the DAG object
        task_list: list of tasks whose SLA was missed
        blocking_task_list: list of tasks blocking the SLA
        slas: list of SlaMiss objects
        blocking_tis: list of TaskInstances blocking completion
    """
    print(f"SLA missed for DAG: {dag.dag_id}")
    for sla in slas:
        print(f"  Task {sla.task_id} missed SLA at {sla.execution_date}")


with DAG(
    dag_id="sla_demo_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # sla_miss_callback: called when any task misses its SLA.
    # REMOVED in 3.x — the callback and SlaMiss table were dropped entirely.
    sla_miss_callback=my_sla_miss_callback,
    tags=["example", "2.10", "sla"],
) as dag:

    start = EmptyOperator(task_id="start")

    def slow_task():
        import time
        time.sleep(1)

    # sla= per-task: if the task doesn't complete within sla of the DAG run start,
    # a SlaMiss record is created and sla_miss_callback is invoked.
    # REMOVED in 3.x — the sla= parameter is no longer valid on BaseOperator.
    critical_task = PythonOperator(
        task_id="critical_task",
        python_callable=slow_task,
        sla=timedelta(hours=1),
    )

    another_task = PythonOperator(
        task_id="another_task",
        python_callable=slow_task,
        sla=timedelta(hours=2),
    )

    start >> critical_task >> another_task