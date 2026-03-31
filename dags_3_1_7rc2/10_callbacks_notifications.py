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
10_callbacks_notifications.py — Airflow 3.1.x style

Migration from 2.10:
- email_on_failure / email_on_retry still exist but emit RemovedInAirflow4Warning
  → prefer SmtpNotifier via on_failure_callback
- EmailOperator moved to providers.smtp (airflow.providers.smtp.operators.smtp)
- execution_date removed from callback context → use logical_date
- on_failure_callback / on_success_callback signatures unchanged
- BaseNotifier pattern is unchanged (introduced in 2.6)
- DAG-level on_execute_callback is new in 3.x
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

# EmailOperator moved to providers.smtp in 3.x.
# Old airflow.operators.email still works (shim) but emits DeprecationWarning.
from airflow.providers.smtp.operators.smtp import EmailOperator

# SmtpNotifier: replaces email_on_failure / email_on_retry in 3.x.
from airflow.providers.smtp.notifications.smtp import SmtpNotifier

# BaseNotifier: unchanged from 2.x.
from airflow.notifications.basenotifier import BaseNotifier


class SlackNotifier(BaseNotifier):
    """Custom notifier — identical pattern to 2.x."""

    def __init__(self, webhook_url: str, message: str):
        self.webhook_url = webhook_url
        self.message = message

    def notify(self, context):
        print(f"Slack notification: {self.message}")
        print(f"  DAG: {context['dag'].dag_id}")
        print(f"  Task: {context['task_instance'].task_id}")


def dag_failure_callback(context):
    print(f"DAG failed: {context['dag'].dag_id}")
    # logical_date replaces execution_date (removed in 3.x).
    print(f"  logical_date: {context['logical_date']}")


def task_failure_callback(context):
    ti = context["task_instance"]
    print(f"Task {ti.task_id} failed in DAG {ti.dag_id}")


def task_success_callback(context):
    ti = context["task_instance"]
    print(f"Task {ti.task_id} succeeded")


def task_retry_callback(context):
    ti = context["task_instance"]
    print(f"Task {ti.task_id} retrying (attempt {ti.try_number})")


with DAG(
    dag_id="callbacks_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=dag_failure_callback,
    on_success_callback=lambda ctx: print(f"DAG succeeded: {ctx['dag'].dag_id}"),
    default_args={
        "owner": "airflow",
        # email_on_failure / email_on_retry still accepted but emit RemovedInAirflow4Warning.
        # PREFERRED: use SmtpNotifier via on_failure_callback instead.
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["example", "3.1", "callbacks"],
) as dag:

    task_with_callbacks = PythonOperator(
        task_id="task_with_callbacks",
        python_callable=lambda: print("doing work"),
        on_failure_callback=task_failure_callback,
        on_success_callback=task_success_callback,
        on_retry_callback=task_retry_callback,
        on_skipped_callback=lambda ctx: print(f"Task {ctx['task_instance'].task_id} was skipped"),
    )

    # EmailOperator from providers.smtp in 3.x.
    # Use logical_date instead of execution_date in templates.
    notify_email = EmailOperator(
        task_id="notify_email",
        to="team@example.com",
        subject="DAG {{ dag.dag_id }} ran on {{ ds }}",
        html_content="<h3>Run complete for {{ logical_date }}</h3>",
    )

    # SmtpNotifier: the preferred alternative to email_on_failure in 3.x.
    task_with_smtp_notifier = PythonOperator(
        task_id="task_with_smtp_notifier",
        python_callable=lambda: print("work"),
        on_failure_callback=SmtpNotifier(
            from_email="airflow@example.com",
            to="alerts@example.com",
            subject="Task {{ task_instance.task_id }} failed",
        ),
    )

    # Custom SlackNotifier (unchanged from 2.x).
    task_with_notifier = EmptyOperator(
        task_id="task_with_notifier",
        on_failure_callback=SlackNotifier(
            webhook_url="https://hooks.slack.com/services/...",
            message="Task failed!",
        ),
    )

    task_with_callbacks >> notify_email >> task_with_smtp_notifier >> task_with_notifier