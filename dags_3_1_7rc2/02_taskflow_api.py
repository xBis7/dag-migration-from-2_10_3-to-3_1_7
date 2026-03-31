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
02_taskflow_api.py — Airflow 3.1.x style

Migration from 2.10:
- Import decorators from airflow.sdk (not airflow.decorators)
- schedule_interval= → schedule=
- execution_date context removed → use logical_date
- @task.run_if and @task.skip_if are new in 3.x
- @task.virtualenv signature is unchanged but now resolves via ProvidersManager
  (from airflow.providers.standard)
- @task.branch now requires BranchPythonOperator from providers.standard
"""

from datetime import datetime

# CANONICAL in 3.x: import all decorators from airflow.sdk
# airflow.decorators still works but emits DeprecationWarning.
from airflow.sdk import dag, setup, task, task_group, teardown


@dag(
    dag_id="taskflow_api_3_1",
    # schedule= replaces schedule_interval=
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1"],
)
def taskflow_pipeline():
    @setup
    @task
    def initialize() -> dict:
        """Setup task — runs before the main tasks."""
        return {"config": "value", "run_id": "abc123"}

    @task
    def extract(**context) -> list[dict]:
        # logical_date replaces execution_date (removed in 3.x).
        logical_date = context["logical_date"]
        print(f"Extracting data for {logical_date}")
        return [{"id": 1, "value": 10}, {"id": 2, "value": 20}]

    @task
    def transform(records: list[dict]) -> list[dict]:
        return [{"id": r["id"], "value": r["value"] * 2} for r in records]

    @task
    def load(records: list[dict]) -> None:
        print(f"Loading {len(records)} records")
        for r in records:
            print(f"  {r}")

    @task.virtualenv(
        requirements=["requests>=2.28"],
        system_site_packages=False,
    )
    def fetch_from_api() -> dict:
        import requests

        return {"status": "ok", "data": [1, 2, 3]}

    # NEW in 3.x: @task.run_if — conditionally skip a task based on a callable.
    # The callable receives the context and returns True to run, False to skip.
    @task.run_if(lambda context: context["logical_date"].weekday() < 5)
    @task
    def weekday_only_task() -> str:
        return "only runs on weekdays"

    # NEW in 3.x: @task.skip_if — inverse of run_if.
    @task.skip_if(lambda context: context["logical_date"].weekday() >= 5)
    @task
    def also_weekday_only() -> str:
        return "skipped on weekends"

    @task_group(group_id="processing")
    def processing_group(records):
        transformed = transform(records)
        load(transformed)

    @teardown
    @task
    def cleanup() -> None:
        """Teardown task — runs after the main tasks."""
        print("Cleaning up resources")

    config = initialize()
    raw_data = extract()
    processing_group(raw_data)
    api_data = fetch_from_api()
    weekday_only_task()
    also_weekday_only()
    cleanup()

    config >> raw_data


taskflow_pipeline()