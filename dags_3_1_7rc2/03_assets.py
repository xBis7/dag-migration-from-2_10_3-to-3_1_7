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
03_assets.py — Airflow 3.1.x style  (was 03_datasets.py in 2.10)

Migration from 2.10:
- Dataset → Asset  (airflow.datasets → airflow.sdk)
- DatasetAll → AssetAll
- DatasetAny → AssetAny
- DatasetAlias → AssetAlias
- DatasetOrTimeSchedule → AssetOrTimeSchedule (new import path)
- triggering_dataset_events → triggering_asset_events
- Asset has new fields: name=, group=, watchers=
- Asset can be identified by name alone (without URI)
- New @asset decorator for defining asset-producing tasks
- New Model class (subclass of Asset) for ML models
- airflow.datasets shims still work until Airflow 3.2 but emit DeprecationWarning
"""

from datetime import datetime

# CANONICAL: import Asset and friends from airflow.sdk
from airflow.sdk import Asset, AssetAlias, AssetAll, AssetAny, dag, task

# AssetOrTimeSchedule replaces DatasetOrTimeSchedule.
# Old path airflow.timetables.datasets still works (shim) until 3.2.
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

# NEW in 3.x: @asset decorator — define a standalone asset-producing function.
# This automatically creates an Asset + a DAG that produces it.
from airflow.sdk.definitions.asset.decorators import asset as asset_decorator

# ------------------------------------------------------------------
# Asset definitions — similar to Dataset but with richer metadata.
# ------------------------------------------------------------------

# Asset with URI only (backward compatible with Dataset(uri=...)).
orders_asset = Asset("s3://my-bucket/orders/{{ ds }}")

# Asset with both name and URI (new in 3.x — name is the human-readable identifier).
summary_asset = Asset(
    name="summary",
    uri="s3://my-bucket/summary/",
)

# Asset with extra (strictly typed as dict[str, JsonValue] — not None).
raw_asset = Asset(
    name="raw_data",
    uri="s3://my-bucket/raw/",
    extra={"format": "parquet", "compression": "snappy"},
    # group= is a new field for asset classification.
    group="data-lake",
)

# Asset identified by name alone (no URI required in 3.x).
named_only_asset = Asset(name="my-named-asset")

# AssetAlias: unchanged in concept, new import path.
alias = AssetAlias("my-alias")

# NEW: Model class — subclass of Asset for ML model tracking.
# from airflow.sdk import Model
# my_model = Model(name="my-model", uri="s3://my-bucket/models/v1")


# ------------------------------------------------------------------
# NEW in 3.x: @asset decorator
# Defines a standalone asset-producing function that becomes its own DAG.
# ------------------------------------------------------------------
@asset_decorator(
    schedule="@hourly",
    uri="s3://my-bucket/decorated-asset/",
    group="decorated",
)
def decorated_orders(context):
    """This function IS the asset — Airflow creates the DAG automatically."""
    print(f"Producing decorated asset at {context['logical_date']}")
    return {"count": 42}


# ------------------------------------------------------------------
# Producer DAG
# ------------------------------------------------------------------
@dag(
    dag_id="asset_producer_3_1",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "assets"],
)
def produce_assets():
    @task(outlets=[orders_asset])
    def produce_orders() -> None:
        print("Producing orders data")

    @task(outlets=[raw_asset])
    def produce_raw() -> None:
        print("Producing raw data")

    @task(outlets=[alias])
    def produce_via_alias(**context) -> None:
        # outlet_events API is the same, but uses AssetAlias.
        context["outlet_events"][alias].add(Asset("s3://my-bucket/dynamic/resolved/"))

    produce_orders()
    produce_raw()
    produce_via_alias()


produce_assets()


# ------------------------------------------------------------------
# Consumer DAG 1: triggered on single asset update
# ------------------------------------------------------------------
@dag(
    dag_id="asset_consumer_single_3_1",
    schedule=orders_asset,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "assets"],
)
def consume_single_asset():
    @task
    def process_orders(**context) -> None:
        # triggering_asset_events replaces triggering_dataset_events (RENAMED in 3.x).
        events = context["triggering_asset_events"]
        for asset_ref, event_list in events.items():
            print(f"Asset {asset_ref} was updated {len(event_list)} time(s)")


consume_single_asset()


# ------------------------------------------------------------------
# Consumer DAG 2: triggered when ALL assets are updated (AssetAll)
# ------------------------------------------------------------------
@dag(
    dag_id="asset_consumer_all_3_1",
    schedule=AssetAll(orders_asset, raw_asset),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "assets"],
)
def consume_all_assets():
    @task
    def process_both() -> None:
        print("Both orders and raw assets are ready")


consume_all_assets()


# ------------------------------------------------------------------
# Consumer DAG 3: triggered when ANY asset is updated (AssetAny)
# ------------------------------------------------------------------
@dag(
    dag_id="asset_consumer_any_3_1",
    schedule=AssetAny(orders_asset, summary_asset),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "assets"],
)
def consume_any_asset():
    @task
    def process_any() -> None:
        print("At least one asset is ready")


consume_any_asset()


# ------------------------------------------------------------------
# Combined timetable + asset schedule (AssetOrTimeSchedule)
# ------------------------------------------------------------------
@dag(
    dag_id="asset_or_time_3_1",
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="UTC"),
        assets=AssetAny(orders_asset, raw_asset),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "3.1", "assets"],
)
def asset_or_time():
    @task
    def run(**context) -> None:
        print("Triggered by time or asset update")


asset_or_time()