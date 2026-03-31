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
03_datasets.py — Airflow 2.10.x style

Demonstrates:
- Dataset class from airflow.datasets (renamed to Asset in 3.x)
- outlet= on tasks marks which datasets are produced
- schedule=Dataset(...) to trigger a DAG when a dataset is updated
- DatasetAll / DatasetAny for compound conditions
- DatasetAlias for dynamic/late-bound dataset references
- triggering_dataset_events context key (renamed in 3.x)
- DatasetOrTimeSchedule for combined time+dataset scheduling
"""

from datetime import datetime

from airflow.datasets import Dataset, DatasetAlias, DatasetAll, DatasetAny

# DatasetOrTimeSchedule: trigger on time OR when a dataset is updated
from airflow.timetables.datasets import DatasetOrTimeSchedule

# DatasetOrTimeSchedule requires a timetable for the time-based side
from airflow.timetables.trigger import CronTriggerTimetable

from airflow.decorators import dag, task

# Define datasets by URI. Only uri= is supported in 2.10 (no name= or group=).
orders_dataset = Dataset("s3://my-bucket/orders/{{ ds }}")
summary_dataset = Dataset("s3://my-bucket/summary/")
raw_dataset = Dataset(
    uri="s3://my-bucket/raw/",
    extra={"format": "parquet"},  # extra is optional metadata
)

# DatasetAlias: a late-bound alias whose actual URI is resolved at runtime.
# Useful when the exact URI is not known at DAG parse time.
alias = DatasetAlias("my-alias")


# Producer DAG: runs on a schedule and marks datasets as produced (outlets).
@dag(
    dag_id="dataset_producer_2_10",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "datasets"],
)
def produce_datasets():
    @task(outlets=[orders_dataset])
    def produce_orders() -> None:
        print("Producing orders data")

    @task(outlets=[raw_dataset])
    def produce_raw() -> None:
        print("Producing raw data")

    # DatasetAlias outlet: the alias is resolved at runtime.
    # The actual dataset URI is set in the task via context["outlet_events"].
    @task(outlets=[alias])
    def produce_via_alias(**context) -> None:
        # In 2.10, you add the event to the alias using outlet_events.
        context["outlet_events"][alias].add(Dataset("s3://my-bucket/dynamic/resolved/"))

    produce_orders()
    produce_raw()
    produce_via_alias()


produce_datasets()


# Consumer DAG 1: triggered when orders_dataset is updated.
@dag(
    dag_id="dataset_consumer_single_2_10",
    # Pass the Dataset directly as schedule to trigger on updates.
    schedule=orders_dataset,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "datasets"],
)
def consume_single_dataset():
    @task
    def process_orders(**context) -> None:
        # triggering_dataset_events: dict of Dataset → list of DatasetEvent
        # RENAMED to triggering_asset_events in 3.x
        events = context["triggering_dataset_events"]
        for dataset, event_list in events.items():
            print(f"Dataset {dataset.uri} was updated {len(event_list)} time(s)")


consume_single_dataset()


# Consumer DAG 2: triggered when BOTH orders AND raw datasets are updated (DatasetAll).
@dag(
    dag_id="dataset_consumer_all_2_10",
    schedule=DatasetAll(orders_dataset, raw_dataset),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "datasets"],
)
def consume_all_datasets():
    @task
    def process_both() -> None:
        print("Both orders and raw datasets are ready")


consume_all_datasets()


# Consumer DAG 3: triggered when EITHER orders OR summary dataset is updated (DatasetAny).
@dag(
    dag_id="dataset_consumer_any_2_10",
    schedule=DatasetAny(orders_dataset, summary_dataset),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "datasets"],
)
def consume_any_dataset():
    @task
    def process_any() -> None:
        print("At least one dataset is ready")


consume_any_dataset()


# Combined timetable + dataset schedule (DatasetOrTimeSchedule).
@dag(
    dag_id="dataset_or_time_2_10",
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        datasets=DatasetAny(orders_dataset, raw_dataset),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "2.10", "datasets"],
)
def dataset_or_time():
    @task
    def run(**context) -> None:
        print("Triggered by time or dataset update")


dataset_or_time()