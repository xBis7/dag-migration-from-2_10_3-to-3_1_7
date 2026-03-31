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
08_dynamic_task_mapping.py — Airflow 2.10.x style

Demonstrates:
- expand() for dynamic task mapping (same in 2.x and 3.x, with minor differences)
- expand_kwargs() for mapping with keyword argument dictionaries
- partial() for static arguments alongside dynamic ones
- map_index_template for labeling mapped tasks
- Dynamic task mapping with TaskFlow @task
"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


@dag(
    dag_id="dynamic_task_mapping_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "mapping"],
)
def dynamic_mapping():
    # Basic expand(): create one task instance per item in the list.
    @task
    def process(item: int) -> int:
        return item * 2

    # expand() maps a single parameter.
    results = process.expand(item=[1, 2, 3, 4, 5])

    # partial() provides static args; expand() provides the dynamic ones.
    @task
    def multiply(value: int, factor: int) -> int:
        return value * factor

    doubled = multiply.partial(factor=2).expand(value=[10, 20, 30])
    tripled = multiply.partial(factor=3).expand(value=[10, 20, 30])

    # expand_kwargs(): each dict in the list maps to one task instance.
    @task
    def process_kwargs(name: str, value: int) -> dict:
        return {"name": name, "result": value * 10}

    mapped = process_kwargs.expand_kwargs([
        {"name": "a", "value": 1},
        {"name": "b", "value": 2},
        {"name": "c", "value": 3},
    ])

    # map_index_template: customize the label shown for each mapped instance in the UI.
    @task(map_index_template="{{ task.op_kwargs['name'] }}")
    def labeled_task(name: str, value: int) -> str:
        return f"{name}={value}"

    labeled = labeled_task.expand_kwargs([
        {"name": "alpha", "value": 100},
        {"name": "beta", "value": 200},
    ])

    # Chaining mapped tasks: each output from process feeds into another task.
    @task
    def summarize(values: list[int]) -> int:
        return sum(values)

    # collect all mapped results and pass to the next task.
    # In 2.x and 3.x, the entire list of results is passed when chained.
    total = summarize(results)

    # expand() also works with PythonOperator (non-TaskFlow).
    mapped_op = PythonOperator.partial(
        task_id="mapped_op",
        python_callable=lambda item: print(f"Processing {item}"),
    ).expand(op_kwargs=[{"item": i} for i in range(3)])


dynamic_mapping()
