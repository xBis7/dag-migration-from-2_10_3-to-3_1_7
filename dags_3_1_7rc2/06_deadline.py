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
06_deadline.py — Airflow 3.1.x style  (replaces 06_sla.py from 2.10)

Migration from SLA (2.10) to DeadlineAlert (3.1):

The entire SLA subsystem was removed in Airflow 3.0:
- sla_miss_callback DAG parameter → removed
- sla= BaseOperator parameter → removed
- SlaMiss database table → removed

In Airflow 3.1, the replacement is deadline= on the DAG with DeadlineAlert:
- DeadlineAlert accepts a timedelta (time from run_after / scheduled start)
- Multiple DeadlineAlerts can be specified as a list
- DeadlineAlert uses the callback/notifier pattern (same as on_failure_callback)
- Per-task SLA equivalent: not directly available — use task-level callbacks instead
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# DeadlineAlert is the 3.1 replacement for SLA.
# Import from airflow.timetables.deadline (added in 3.1).
from airflow.timetables.deadline import DeadlineAlert


def my_deadline_callback(context):
    """
    Called when a DAG run exceeds its deadline.

    Unlike sla_miss_callback (2.x), this is a standard Airflow callback:
    it receives the task context dict (same as on_failure_callback).
    """
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    logical_date = context["logical_date"]
    print(f"Deadline exceeded for DAG {dag_id}, run {run_id} (logical={logical_date})")


with DAG(
    dag_id="deadline_demo_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # deadline= replaces sla_miss_callback + per-task sla=.
    # Pass a DeadlineAlert or a list of DeadlineAlerts.
    # DeadlineAlert(timedelta): fires if the DAG run does not complete within
    # the given timedelta from run_after (the scheduled start time).
    deadline=DeadlineAlert(
        timedelta(hours=2),
        # callback= is optional; if omitted, uses the DAG's on_failure_callback.
        callback=my_deadline_callback,
    ),
    # Multiple deadlines: e.g., warn at 1h, escalate at 2h.
    # deadline=[
    #     DeadlineAlert(timedelta(hours=1), callback=warn_callback),
    #     DeadlineAlert(timedelta(hours=2), callback=escalate_callback),
    # ],
    tags=["example", "3.1", "deadline"],
) as dag:

    start = EmptyOperator(task_id="start")

    def slow_task():
        import time
        time.sleep(1)

    # In 3.x, there is no per-task deadline equivalent of sla=.
    # For per-task alerting, use on_failure_callback / on_retry_callback.
    critical_task = PythonOperator(
        task_id="critical_task",
        python_callable=slow_task,
        # Per-task callback as a substitute for per-task sla=:
        on_failure_callback=lambda context: print(
            f"Task {context['task_instance'].task_id} failed"
        ),
    )

    another_task = PythonOperator(
        task_id="another_task",
        python_callable=slow_task,
    )

    start >> critical_task >> another_task
