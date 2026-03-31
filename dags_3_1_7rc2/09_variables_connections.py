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
09_variables_connections.py — Airflow 3.1.x style

Migration from 2.10:
- Variable.get() and BaseHook.get_connection() API surface is UNCHANGED.
  However, the execution path changed: in 3.x workers communicate via the
  Execution API (HTTP to the API server) instead of directly hitting the DB.
  This is transparent to DAG code — same function calls, different internals.
- Import Variable from airflow.sdk (canonical) or airflow.models (shim).
- Connection import path: airflow.sdk.Connection or airflow.models.connection.
- Jinja template syntax for var/conn is unchanged.

Behavioral differences:
- Latency: Variable/Connection reads now make HTTP calls (Execution API).
  For high-frequency reads inside tight loops, consider caching locally.
- Secrets backends: still supported, accessed transparently via Execution API.
- Serialization: Variable values stored as JSON (not pickled) in 3.x.
"""

from datetime import datetime

from airflow.sdk import DAG

# CANONICAL: import from airflow.sdk
# airflow.models.Variable still works (shim) but use sdk for forward compatibility.
from airflow.sdk import Variable

# Connection import: airflow.sdk.Connection is available in 3.x.
from airflow.hooks.base import BaseHook  # still works in 3.x

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


def use_variable(**context):
    # Variable.get() — same API as 2.10, different execution path.
    # In 3.x: request goes through Execution API → API server → DB/secrets backend.
    # The DAG code is IDENTICAL to 2.10.
    my_var = Variable.get("my_variable", default_var="default_value")
    json_var = Variable.get("my_json_var", deserialize_json=True, default_var={})
    print(f"my_var={my_var}, json_var={json_var}")


def use_connection(**context):
    # BaseHook.get_connection() — same API, goes through Execution API in 3.x.
    conn = BaseHook.get_connection("my_db_connection")
    print(f"host={conn.host}, schema={conn.schema}, login={conn.login}")
    uri = conn.get_uri()
    print(f"uri={uri}")


with DAG(
    dag_id="variables_connections_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "variables"],
) as dag:

    task_use_variable = PythonOperator(
        task_id="use_variable",
        python_callable=use_variable,
    )

    task_use_connection = PythonOperator(
        task_id="use_connection",
        python_callable=use_connection,
    )

    # Jinja templates for Variables and Connections — UNCHANGED from 2.10.
    bash_with_var = BashOperator(
        task_id="bash_with_var",
        bash_command=(
            "echo 'var={{ var.value.my_variable }}' && "
            "echo 'host={{ conn.my_db_connection.host }}'"
        ),
    )

    task_use_variable >> task_use_connection >> bash_with_var