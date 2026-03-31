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
05_task_groups_and_branches.py — Airflow 3.1.x style

Migration from 2.10:
- SubDagOperator REMOVED → use TaskGroup instead
- BranchPythonOperator moved to providers.standard
- ShortCircuitOperator moved to providers.standard
- BranchDateTimeOperator moved to providers.standard
- LatestOnlyOperator moved to providers.standard
- execution_date removed from context → use logical_date
- All old airflow.operators.* imports emit DeprecationWarning
"""

from datetime import datetime

from airflow.sdk import DAG, task_group

# All operators are now in providers.standard in 3.x.
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

# SubDagOperator is GONE in 3.x. TaskGroup is the replacement.
# from airflow.utils.task_group import TaskGroup  — low-level import
# from airflow.sdk import task_group  — decorator-based (TaskFlow API)

DAG_ID = "task_groups_and_branches_3_1"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # TaskGroup: replaces SubDagOperator in 3.x.
    # TaskGroups are purely visual/logical groupings — no separate DAG execution.
    # They are lightweight, don't spin up separate workers, and share the same DagRun.
    from airflow.utils.task_group import TaskGroup

    with TaskGroup("my_group") as group:
        group_task_1 = EmptyOperator(task_id="group_task_1")
        group_task_2 = EmptyOperator(task_id="group_task_2")
        group_task_1 >> group_task_2

    def choose_branch(**context):
        # logical_date replaces execution_date (removed in 3.x).
        logical_date = context["logical_date"]
        if logical_date.weekday() < 5:
            return "weekday_task"
        return "weekend_task"

    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=choose_branch,
    )

    weekday_task = EmptyOperator(task_id="weekday_task")
    weekend_task = EmptyOperator(task_id="weekend_task")

    def is_large_dataset(**context) -> bool:
        return True

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit",
        python_callable=is_large_dataset,
        ignore_downstream_trigger_rules=True,
    )

    datetime_branch = BranchDateTimeOperator(
        task_id="datetime_branch",
        follow_task_ids_if_true=["weekday_task"],
        follow_task_ids_if_false=["weekend_task"],
        target_upper=datetime(2099, 12, 31),
        target_lower=datetime(2000, 1, 1),
    )

    latest_only = LatestOnlyOperator(task_id="latest_only")

    start >> group >> branch
    branch >> [weekday_task, weekend_task] >> end
    start >> short_circuit >> end
    start >> latest_only >> end