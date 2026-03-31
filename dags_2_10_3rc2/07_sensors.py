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
07_sensors.py — Airflow 2.10.x style

Demonstrates:
- BaseSensorOperator from airflow.sensors.base (deprecated path in 3.x)
- PokeReturnValue for returning data from a sensor
- TimeDeltaSensor and TimeDeltaSensorAsync (merged in 3.x)
- ExternalTaskSensor
- FileSensor, DateTimeSensor, TimeSensor
- deferrable=True sensors (same in 3.x but execution path changed)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# BaseSensorOperator in 2.x is in airflow.sensors.base.
# In 3.x: from airflow.sdk import BaseSensorOperator
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue

# These sensors moved to providers.standard in 3.x.
from airflow.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.time_sensor import TimeSensor


class MyCustomSensor(BaseSensorOperator):
    """
    Custom sensor: poke checks a condition, returns PokeReturnValue with data.
    The PokeReturnValue mechanism (returning data from a sensor) works the same
    in 2.x and 3.x, but the import path for BaseSensorOperator changes.
    """

    def __init__(self, my_param: str, **kwargs):
        super().__init__(**kwargs)
        self.my_param = my_param

    def poke(self, context) -> bool | PokeReturnValue:
        # If the condition is met, return PokeReturnValue(is_done=True, xcom_value=...)
        # to both signal success and push data to XCom.
        if self.my_param == "ready":
            return PokeReturnValue(is_done=True, xcom_value={"status": "ready"})
        return False


with DAG(
    dag_id="sensors_demo_2_10",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "sensors"],
) as dag:

    # TimeDeltaSensor: waits for a timedelta after the DAG run start.
    wait_5min = TimeDeltaSensor(
        task_id="wait_5min",
        delta=timedelta(minutes=5),
    )

    # TimeDeltaSensorAsync: async (deferrable) variant of TimeDeltaSensor.
    # In 3.x, TimeDeltaSensorAsync was MERGED INTO TimeDeltaSensor.
    # Use deferrable=True on TimeDeltaSensor in 3.x instead.
    wait_async = TimeDeltaSensorAsync(
        task_id="wait_async",
        delta=timedelta(minutes=5),
    )

    # ExternalTaskSensor: waits for a task in another DAG to complete.
    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_dag",
        external_task_id="final_task",
        # execution_date_fn: maps current execution_date to upstream execution_date.
        # execution_date context variable removed in 3.x.
        execution_date_fn=lambda dt: dt,
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # FileSensor: waits for a file to exist on the filesystem.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/tmp/my_data_{{ ds }}.csv",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    # DateTimeSensor: waits until a specific datetime.
    wait_until = DateTimeSensor(
        task_id="wait_until",
        target_time="{{ execution_date.replace(hour=9, minute=0) }}",
    )

    # Custom sensor usage.
    my_sensor = MyCustomSensor(
        task_id="my_sensor",
        my_param="ready",
        poke_interval=10,
        timeout=300,
        mode="poke",  # or "reschedule" to free the worker slot while waiting
        # soft_fail=True: mark task as SKIPPED instead of FAILED on timeout.
        soft_fail=False,
        # exponential_backoff=True: increase poke_interval exponentially over time.
        exponential_backoff=False,
    )

    done = EmptyOperator(task_id="done")

    [wait_5min, wait_async, wait_for_upstream, wait_for_file, wait_until, my_sensor] >> done
