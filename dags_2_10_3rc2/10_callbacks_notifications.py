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
10_callbacks_notifications.py — Airflow 2.10.x style

Demonstrates:
- on_failure_callback, on_success_callback, on_retry_callback on DAG and tasks
- on_skipped_callback (added in 2.x, still valid in 3.x)
- email_on_failure / email_on_retry on BaseOperator (deprecated in 3.x)
- EmailOperator from airflow.operators.email (moved to providers.smtp in 3.x)
- send_email utility from airflow.utils.email (may move in 3.x)
- Notifier pattern (introduced in 2.6, same in 3.x)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# EmailOperator in 2.x: from airflow.operators.email
# In 3.x: from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.operators.email import EmailOperator

# Notification framework (introduced in Airflow 2.6, same in 3.x).
from airflow.notifications.basenotifier import BaseNotifier


class SlackNotifier(BaseNotifier):
    """Example custom notifier. The Notifier pattern is identical in 2.x and 3.x."""

    def __init__(self, webhook_url: str, message: str):
        self.webhook_url = webhook_url
        self.message = message

    def notify(self, context):
        # In real use: send HTTP request to Slack webhook.
        print(f"Slack notification: {self.message}")
        print(f"  DAG: {context['dag'].dag_id}")
        print(f"  Task: {context['task_instance'].task_id}")


def dag_failure_callback(context):
    """DAG-level failure callback."""
    print(f"DAG failed: {context['dag'].dag_id}")
    # execution_date still available in 2.10 context (REMOVED in 3.x).
    print(f"  execution_date: {context['execution_date']}")


def task_failure_callback(context):
    """Task-level failure callback."""
    ti = context["task_instance"]
    print(f"Task {ti.task_id} failed in DAG {ti.dag_id}")


def task_success_callback(context):
    """Task-level success callback."""
    ti = context["task_instance"]
    print(f"Task {ti.task_id} succeeded")


def task_retry_callback(context):
    """Task-level retry callback."""
    ti = context["task_instance"]
    print(f"Task {ti.task_id} retrying (attempt {ti.try_number})")


with DAG(
    dag_id="callbacks_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # DAG-level callbacks (same in 3.x).
    on_failure_callback=dag_failure_callback,
    # DAG-level success callback runs when all tasks succeed.
    on_success_callback=lambda ctx: print(f"DAG succeeded: {ctx['dag'].dag_id}"),
    default_args={
        "owner": "airflow",
        # email_on_failure / email_on_retry: still in 2.x, deprecated in 3.x.
        # In 3.x they emit RemovedInAirflow4Warning — use Notifiers instead.
        "email": ["alerts@example.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["example", "2.10", "callbacks"],
) as dag:

    task_with_callbacks = PythonOperator(
        task_id="task_with_callbacks",
        python_callable=lambda: print("doing work"),
        # Task-level callbacks.
        on_failure_callback=task_failure_callback,
        on_success_callback=task_success_callback,
        on_retry_callback=task_retry_callback,
        # on_skipped_callback: called when the task is skipped.
        on_skipped_callback=lambda ctx: print(f"Task {ctx['task_instance'].task_id} was skipped"),
    )

    # EmailOperator in 2.x (moved to providers.smtp in 3.x).
    notify_email = EmailOperator(
        task_id="notify_email",
        to="team@example.com",
        subject="DAG {{ dag.dag_id }} ran on {{ ds }}",
        html_content="<h3>Run complete for {{ execution_date }}</h3>",
    )

    # Notifier pattern (same in 2.x and 3.x — introduced in 2.6).
    task_with_notifier = EmptyOperator(
        task_id="task_with_notifier",
        on_failure_callback=SlackNotifier(
            webhook_url="https://hooks.slack.com/services/...",
            message="Task failed!",
        ),
    )

    task_with_callbacks >> notify_email >> task_with_notifier