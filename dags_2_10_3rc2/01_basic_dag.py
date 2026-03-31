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
01_basic_dag.py — Airflow 2.10.x style

Demonstrates:
- DAG creation with schedule_interval (deprecated in 2.10, removed in 3.x)
- catchup=True was the default in 2.x
- default_view and orientation (removed in 3.x)
- concurrency parameter (removed in 3.x; use max_active_tasks)
- fail_stop parameter (renamed to fail_fast in 3.x)
- Imports from airflow.models.dag (deprecated in 3.x)
- Operators imported from airflow.operators.* (deprecated in 3.x)
- Context variable execution_date (removed in 3.x)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# In 2.10 the DAG class is imported from airflow directly (or airflow.models.dag).
# Both work, but in 3.x the canonical import is: from airflow.sdk import DAG

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # email_on_failure / email_on_retry are BaseOperator params in 2.x
    # In 3.x they still exist but emit RemovedInAirflow4Warning; use Notifiers instead
    "email_on_failure": False,
    "email_on_retry": False,
    # sla per-task is still accepted in 2.10 (BaseSensorOperator / BaseOperator param)
    # removed or no-op in 3.x
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="basic_dag_2_10",
    default_args=default_args,
    description="Basic DAG for Airflow 2.10",
    # schedule_interval is the 2.x parameter. Emits RemovedInAirflow3Warning in 2.10.
    # REMOVED in 3.x — use schedule= instead.
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    # catchup=True was the old default. Explicitly set here to highlight the change:
    # in 3.x it defaults to False (config scheduler/catchup_by_default=False).
    catchup=True,
    # default_view controls which tab opens in the UI per DAG.
    # REMOVED in 3.x (migration 0062 drops the column).
    default_view="graph",
    # orientation controls graph view layout direction.
    # REMOVED in 3.x.
    orientation="LR",
    # concurrency limits how many task instances can run concurrently within this DAG.
    # REMOVED in 3.x — use max_active_tasks= instead.
    concurrency=4,
    # fail_stop: if any task fails, immediately fail all running tasks.
    # RENAMED to fail_fast= in 3.x.
    fail_stop=False,
    # sla_miss_callback: called when an SLA is missed.
    # REMOVED in 3.x — SLA feature dropped; see DeadlineAlert in 3.1.
    sla_miss_callback=None,
    tags=["example", "2.10"],
) as dag:

    start = EmptyOperator(task_id="start")

    def print_context(**context):
        # execution_date is available in 2.10 context.
        # REMOVED in 3.x — use logical_date instead.
        execution_date = context["execution_date"]
        # next_execution_date — REMOVED in 3.x; use data_interval_end.
        next_exec = context["next_execution_date"]
        # prev_execution_date — REMOVED in 3.x; no direct equivalent.
        prev_exec = context["prev_execution_date"]
        # next_ds — REMOVED in 3.x; use {{ data_interval_end | ds }}.
        next_ds = context["next_ds"]
        print(f"execution_date={execution_date}, next={next_exec}, prev={prev_exec}, next_ds={next_ds}")

    print_ctx = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        # provide_context=True was required in Airflow 1.x; it's the default in 2.x+
        # but still accepted. REMOVED in 3.x — context is always injected.
        provide_context=True,
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'execution_date={{ execution_date }}'",
        # task_concurrency limited how many runs of this task can run at once (per-task).
        # REMOVED in 3.x — use max_active_tis_per_dag= (already renamed in 2.x).
        task_concurrency=2,
    )

    end = EmptyOperator(task_id="end")

    start >> print_ctx >> bash_task >> end