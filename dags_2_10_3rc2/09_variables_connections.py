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
09_variables_connections.py — Airflow 2.10.x style

Demonstrates:
- Variable.get() — direct DB access from within tasks in 2.x
  (goes through Execution API in 3.x instead)
- Connection.get_connection_from_secrets() — direct DB access in 2.x
- Jinja template access to Variables: {{ var.value.my_var }}
- Jinja template access to Connections: {{ conn.my_conn.host }}
- BaseHook.get_connection() — same DB in 2.x, Execution API in 3.x

Note: The API surface for Variable and Connection is unchanged in 3.x,
but the underlying execution mechanism changed: workers no longer hit
the DB directly; calls go through the Execution API / API server.
"""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def use_variable(**context):
    # Variable.get() in 2.x: reads directly from the metadata DB.
    # In 3.x: same API but request goes through the Execution API (HTTP).
    my_var = Variable.get("my_variable", default_var="default_value")
    json_var = Variable.get("my_json_var", deserialize_json=True, default_var={})
    print(f"my_var={my_var}, json_var={json_var}")


def use_connection(**context):
    # BaseHook.get_connection() in 2.x: reads from DB directly.
    # In 3.x: same API but goes through Execution API.
    conn = BaseHook.get_connection("my_db_connection")
    print(f"host={conn.host}, schema={conn.schema}, login={conn.login}")

    # get_uri() returns a connection URI string.
    uri = conn.get_uri()
    print(f"uri={uri}")


with DAG(
    dag_id="variables_connections_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "variables"],
) as dag:

    task_use_variable = PythonOperator(
        task_id="use_variable",
        python_callable=use_variable,
    )

    task_use_connection = PythonOperator(
        task_id="use_connection",
        python_callable=use_connection,
    )

    # Jinja templates for Variables and Connections — same syntax in 2.x and 3.x.
    bash_with_var = BashOperator(
        task_id="bash_with_var",
        # {{ var.value.my_variable }} reads a Variable via Jinja.
        # {{ var.json.my_json_var }} reads and deserializes a JSON Variable.
        # {{ conn.my_db_connection.host }} reads a Connection field via Jinja.
        bash_command=(
            "echo 'var={{ var.value.my_variable }}' && "
            "echo 'host={{ conn.my_db_connection.host }}'"
        ),
    )

    task_use_variable >> task_use_connection >> bash_with_var
