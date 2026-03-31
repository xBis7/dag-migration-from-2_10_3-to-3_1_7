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
08_dynamic_task_mapping.py — Airflow 3.1.x style

Migration from 2.10:
- Dynamic task mapping API (expand/partial/expand_kwargs) is largely unchanged
- Import decorators from airflow.sdk (not airflow.decorators)
- Import PythonOperator from providers.standard
- schedule_interval= → schedule=
- XCom values must be JSON-serializable (affects list results from mapped tasks)

Note: Dynamic task mapping itself did not change significantly between 2.10 and 3.x.
The main migration concerns are import paths and XCom JSON serialization.
"""

from datetime import datetime

# CANONICAL imports in 3.x
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator


@dag(
    dag_id="dynamic_task_mapping_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "mapping"],
)
def dynamic_mapping():
    @task
    def process(item: int) -> int:
        return item * 2

    results = process.expand(item=[1, 2, 3, 4, 5])

    @task
    def multiply(value: int, factor: int) -> int:
        return value * factor

    doubled = multiply.partial(factor=2).expand(value=[10, 20, 30])
    tripled = multiply.partial(factor=3).expand(value=[10, 20, 30])

    @task
    def process_kwargs(name: str, value: int) -> dict:
        return {"name": name, "result": value * 10}

    mapped = process_kwargs.expand_kwargs([
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
        {"name": "c", "value": 3},
    ])

    @task(map_index_template="{{ task.op_kwargs['name'] }}")
    def labeled_task(name: str, value: int) -> str:
        return f"{name}={value}"

    labeled = labeled_task.expand_kwargs([
        {"name": "alpha", "value": 100},
        {"name": "beta", "value": 200},
    ])

    @task
    def summarize(values: list[int]) -> int:
        # In 3.x, values come through XCom as JSON — list[int] is fine.
        return sum(values)

    total = summarize(results)

    # PythonOperator from providers.standard in 3.x.
    mapped_op = PythonOperator.partial(
        task_id="mapped_op",
        python_callable=lambda item: print(f"Processing {item}"),
    ).expand(op_kwargs=[{"item": i} for i in range(3)])


dynamic_mapping()
