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
02_taskflow_api.py — Airflow 2.10.x style

Demonstrates:
- @dag and @task decorators from airflow.decorators (deprecated path in 3.x)
- @task.virtualenv
- @task_group from airflow.utils.task_group (deprecated path in 3.x)
- setup/teardown tasks (introduced in 2.10, still valid in 3.x)
- XCom push/pull (implicit via return values)
- Context variable access: execution_date (removed in 3.x)
"""

from datetime import datetime

# 2.10 canonical decorator imports. In 3.x these still work (shims) but emit
# DeprecationWarning — the canonical import is: from airflow.sdk import dag, task, task_group
from airflow.decorators import dag, task, task_group

# setup/teardown were introduced in Airflow 2.7 and are still present in 3.x.
from airflow.decorators import setup, teardown


@dag(
    dag_id="taskflow_api_2_10",
    # schedule_interval — REMOVED in 3.x; use schedule=
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10"],
)
def taskflow_pipeline():
    @setup
    @task
    def initialize() -> dict:
        """Setup task — runs before the main tasks."""
        return {"config": "value", "run_id": "abc123"}

    @task
    def extract(**context) -> list[dict]:
        # execution_date is available in 2.10 context. REMOVED in 3.x.
        execution_date = context["execution_date"]
        print(f"Extracting data for {execution_date}")
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
        # In 2.10 and 3.x: requirements can be a list of packages
        requirements=["requests>=2.28"],
        # system_site_packages allows access to host Python packages inside venv
        system_site_packages=False,
    )
    def fetch_from_api() -> dict:
        import requests  # available because it's in requirements

        # This runs in an isolated virtualenv
        return {"status": "ok", "data": [1, 2, 3]}

    @task_group(group_id="processing")
    def processing_group(records):
        transformed = transform(records)
        load(transformed)

    @teardown
    @task
    def cleanup() -> None:
        """Teardown task — runs after the main tasks."""
        print("Cleaning up resources")

    # Wiring tasks together
    config = initialize()
    raw_data = extract()
    processing_group(raw_data)
    api_data = fetch_from_api()
    cleanup()

    # setup/teardown relationship
    config >> raw_data


taskflow_pipeline()