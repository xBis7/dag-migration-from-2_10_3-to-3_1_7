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
05_subdag_and_branches.py — Airflow 2.10.x style

Demonstrates:
- SubDagOperator — REMOVED in 3.x; use TaskGroup instead
- BranchPythonOperator from airflow.operators.python
- ShortCircuitOperator from airflow.operators.python
- BranchDateTimeOperator from airflow.operators.datetime
- LatestOnlyOperator from airflow.operators.latest_only
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.latest_only import LatestOnlyOperator

# SubDagOperator: Allows embedding a DAG inside another DAG as a single task.
# REMOVED in 3.x — the is_subdag column and root_dag_id were dropped in migration 0029.
# In 3.x use TaskGroup to logically group tasks instead.
from airflow.operators.subdag import SubDagOperator


# Helper: define a subdag factory function.
def create_subdag(parent_dag_id, child_dag_id, start_date, schedule_interval):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        schedule_interval=schedule_interval,
        start_date=start_date,
    ) as subdag:
        EmptyOperator(task_id="subdag_task_1")
        EmptyOperator(task_id="subdag_task_2")
    return subdag


DAG_ID = "subdag_and_branches_2_10"

with DAG(
    dag_id=DAG_ID,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # SubDagOperator: REMOVED in 3.x.
    # Use TaskGroup in 3.x instead (see 05_task_groups_and_branches.py).
    subdag = SubDagOperator(
        task_id="my_subdag",
        subdag=create_subdag(
            parent_dag_id=DAG_ID,
            child_dag_id="my_subdag",
            start_date=datetime(2024, 1, 1),
            schedule_interval="@daily",
        ),
    )

    def choose_branch(**context):
        # execution_date still available in 2.10.
        execution_date = context["execution_date"]
        if execution_date.weekday() < 5:
            return "weekday_task"
        return "weekend_task"

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_branch,
    )

    weekday_task = EmptyOperator(task_id="weekday_task")
    weekend_task = EmptyOperator(task_id="weekend_task")

    def is_large_dataset(**context) -> bool:
        return True  # if False, all downstream tasks are skipped

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit",
        python_callable=is_large_dataset,
        # ignore_downstream_trigger_rules: if True (default), overrides trigger rules
        # so ALL downstream tasks are skipped, not just direct children.
        ignore_downstream_trigger_rules=True,
    )

    # BranchDateTimeOperator: branches based on current time.
    datetime_branch = BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=["weekday_task"],
        follow_task_ids_if_false=["weekend_task"],
        target_upper=datetime(2099, 12, 31),
        target_lower=datetime(2000, 1, 1),
    )

    # LatestOnlyOperator: skips downstream tasks for all runs except the latest.
    # Useful when catchup=True but you only want the most recent run to do work.
    latest_only = LatestOnlyOperator(task_id="latest_only")

    start >> subdag >> branch
    branch >> [weekday_task, weekend_task] >> end
    start >> short_circuit >> end
    start >> latest_only >> end