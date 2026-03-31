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
07_sensors.py — Airflow 3.1.x style

Migration from 2.10:
- BaseSensorOperator moved: from airflow.sdk import BaseSensorOperator
  (old airflow.sensors.base still works but emits DeprecationWarning)
- All sensors moved to airflow.providers.standard.sensors.*
- TimeDeltaSensorAsync MERGED INTO TimeDeltaSensor: use deferrable=True
- ExternalTaskSensor execution_date_fn/execution_date removed → use logical_date_fn
- Sensor execution goes through Task SDK (Execution API), not direct DB
- workers no longer call DB directly for Variable.get, Connection, etc.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG

from airflow.providers.standard.operators.empty import EmptyOperator

# CANONICAL: import BaseSensorOperator from airflow.sdk
# Old airflow.sensors.base still works (shim) but emits DeprecationWarning.
from airflow.sdk import BaseSensorOperator, PokeReturnValue

# All sensors moved to providers.standard in 3.x.
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

# TimeDeltaSensorAsync was MERGED INTO TimeDeltaSensor. Use deferrable=True.
# from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync  # GONE

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.providers.standard.sensors.time import TimeSensor


class MyCustomSensor(BaseSensorOperator):
    """
    Custom sensor — same logic as 2.10 but now uses airflow.sdk base class.
    PokeReturnValue works identically.
    """

    def __init__(self, my_param: str, **kwargs):
        super().__init__(**kwargs)
        self.my_param = my_param

    def poke(self, context) -> bool | PokeReturnValue:
        if self.my_param == "ready":
            return PokeReturnValue(is_done=True, xcom_value={"status": "ready"})
        return False


with DAG(
    dag_id="sensors_demo_3_1",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "sensors"],
) as dag:

    # TimeDeltaSensor: unchanged.
    wait_5min = TimeDeltaSensor(
        task_id="wait_5min",
        delta=timedelta(minutes=5),
    )

    # TimeDeltaSensorAsync is GONE in 3.x.
    # Use TimeDeltaSensor with deferrable=True instead.
    wait_async = TimeDeltaSensor(
        task_id="wait_async",
        delta=timedelta(minutes=5),
        # deferrable=True makes it non-blocking (equivalent to TimeDeltaSensorAsync).
        deferrable=True,
    )

    # ExternalTaskSensor: execution_date_fn renamed to logical_date_fn in 3.x.
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_dag",
        external_task_id="final_task",
        # logical_date_fn replaces execution_date_fn (execution_date removed in 3.x).
        logical_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # FileSensor: unchanged in 3.x (just different import path).
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/my_data_{{ ds }}.csv",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # DateTimeSensor: use logical_date instead of execution_date in templates.
    wait_until = DateTimeSensor(
        task_id="wait_until",
        target_time="{{ logical_date.replace(hour=9, minute=0) }}",
    )

    my_sensor = MyCustomSensor(
        task_id="my_sensor",
        my_param="ready",
        poke_interval=10,
        timeout=300,
        mode="poke",
        soft_fail=False,
        exponential_backoff=False,
    )

    done = EmptyOperator(task_id="done")

    [wait_5min, wait_async, wait_for_upstream, wait_for_file, wait_until, my_sensor] >> done
