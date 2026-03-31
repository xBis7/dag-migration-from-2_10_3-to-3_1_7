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
01_basic_dag.py — Airflow 3.1.x style

Migration from 2.10:
- schedule_interval= → schedule=
- concurrency= → max_active_tasks=
- fail_stop= → fail_fast=
- default_view=, orientation= removed entirely
- sla_miss_callback= removed (use deadline= with DeadlineAlert)
- execution_date context variable removed → use logical_date / data_interval_start
- next_execution_date removed → use data_interval_end
- prev_execution_date removed (no direct equivalent)
- next_ds/prev_ds removed → use {{ data_interval_end | ds }} in templates
- provide_context=True removed from PythonOperator (context always injected)
- task_concurrency= removed → use max_active_tis_per_dag=
- Canonical import: from airflow.sdk import DAG
- Operators moved to airflow.providers.standard.*
- catchup defaults to False
"""

from datetime import datetime, timedelta

# CANONICAL in 3.x: import DAG from airflow.sdk
# from airflow import DAG still works (re-export shim) but airflow.sdk is the source of truth.
from airflow.sdk import DAG

# Operators moved to providers.standard in 3.x.
# Old airflow.operators.* imports still work but emit DeprecationWarning.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # email_on_failure / email_on_retry still exist but emit RemovedInAirflow4Warning.
    # Prefer using SmtpNotifier via on_failure_callback / on_retry_callback.
    "email_on_failure": False,
    "email_on_retry": False,
    # sla= is NO LONGER a valid BaseOperator parameter in 3.x.
    # Use deadline= on the DAG with DeadlineAlert (see 06_deadline.py).
}

with DAG(
    dag_id="basic_dag_3_1",
    default_args=default_args,
    description="Basic DAG for Airflow 3.1",
    # schedule= replaces schedule_interval=. Accepts cron strings, timedelta,
    # timetable instances, Asset references, or None (no schedule).
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    # catchup defaults to False in 3.x (config scheduler/catchup_by_default=False).
    # Explicitly set to True here to match 2.10 behaviour if needed.
    catchup=False,
    # default_view= REMOVED. The UI no longer supports per-DAG default views.
    # orientation= REMOVED.
    # concurrency= REMOVED. Use max_active_tasks= instead.
    max_active_tasks=4,
    # fail_fast= replaces fail_stop= (renamed in 3.x).
    fail_fast=False,
    # sla_miss_callback= REMOVED. Use deadline= with DeadlineAlert (3.1+).
    # deadline=DeadlineAlert(timedelta(hours=2)),  # see 06_deadline.py
    tags=["example", "3.1"],
) as dag:

    start = EmptyOperator(task_id="start")

    def print_context(**context):
        # execution_date REMOVED from context in 3.x.
        # Use logical_date (equivalent to old execution_date = data_interval_start).
        logical_date = context["logical_date"]
        # data_interval_end replaces next_execution_date.
        data_interval_end = context["data_interval_end"]
        # data_interval_start is the start of the data window.
        data_interval_start = context["data_interval_start"]
        # For template equivalent of next_ds: {{ data_interval_end | ds }}
        next_ds_equiv = data_interval_end.strftime("%Y-%m-%d") if data_interval_end else None
        print(
            f"logical_date={logical_date}, "
            f"data_interval_start={data_interval_start}, "
            f"data_interval_end={data_interval_end}, "
            f"next_ds_equiv={next_ds_equiv}"
        )

    print_ctx = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
        # provide_context=True REMOVED in 3.x. Context is always passed as **kwargs.
    )

    bash_task = BashOperator(
        task_id="bash_task",
        # Use logical_date in templates instead of execution_date.
        bash_command="echo 'logical_date={{ logical_date }}'",
        # max_active_tis_per_dag= replaces task_concurrency= (already renamed in 2.x).
        max_active_tis_per_dag=2,
    )

    end = EmptyOperator(task_id="end")

    start >> print_ctx >> bash_task >> end