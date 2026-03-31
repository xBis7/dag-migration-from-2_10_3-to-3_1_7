# Airflow 2.10.3 → 3.1.7 DAG Migration Reference

For comparison, the following branches were used

* branch: `2.10.3rc2`
  * https://github.com/apache/airflow/tree/2.10.3rc2
  * last commit SHA: `c99887ec11ce3e1a43f2794fcf36d27555140f00`

* branch: `3.1.7rc2`
  * https://github.com/apache/airflow/tree/3.1.7rc2
  * last commit SHA: `83ff6ecec9dd71d8b8631248bc0725afc472acd7`

## Quick-scan error index

If you see a specific error or warning, find it here first.

| Error / Warning | Cause | Fix |
|---|---|---|
| `TypeError: DAG.__init__() got unexpected keyword 'schedule_interval'` | Removed param | `schedule_interval="@daily"` → `schedule="@daily"` |
| `TypeError: DAG.__init__() got unexpected keyword 'concurrency'` | Removed param | `concurrency=N` → `max_active_tasks=N` |
| `TypeError: DAG.__init__() got unexpected keyword 'fail_stop'` | Renamed param | `fail_stop=True` → `fail_fast=True` |
| `TypeError: DAG.__init__() got unexpected keyword 'default_view'` | Removed param | Delete the parameter entirely |
| `TypeError: DAG.__init__() got unexpected keyword 'orientation'` | Removed param | Delete the parameter entirely |
| `TypeError: DAG.__init__() got unexpected keyword 'sla_miss_callback'` | Removed param | Delete or use `deadline=DeadlineAlert(...)` |
| `TypeError: BaseOperator.__init__() got unexpected keyword 'task_concurrency'` | Removed param | `task_concurrency=N` → `max_active_tis_per_dag=N` |
| `TypeError: BaseOperator.__init__() got unexpected keyword 'sla'` | Removed param | Delete per-task `sla=` |
| `KeyError: 'execution_date'` in context | Context key removed | Use `logical_date` |
| `KeyError: 'next_execution_date'` in context | Context key removed | Use `data_interval_end` |
| `KeyError: 'prev_execution_date'` in context | Context key removed | No direct equivalent |
| `KeyError: 'next_ds'` / `'prev_ds'` in context | Context keys removed | `{{ data_interval_end \| ds }}` |
| `KeyError: 'triggering_dataset_events'` in context | Renamed | Use `triggering_asset_events` |
| `ImportError: cannot import name 'SubDagOperator' from 'airflow.operators.subdag'` | Removed | Use `TaskGroup` |
| `DeprecationWarning: from airflow.datasets import Dataset` | Renamed module | `from airflow.sdk import Asset` |
| `DeprecationWarning: from airflow.operators.python import PythonOperator` | Moved module | `from airflow.providers.standard.operators.python import PythonOperator` |
| `DeprecationWarning: from airflow.decorators import dag, task` | Moved module | `from airflow.sdk import dag, task` |
| `DeprecationWarning: from airflow.models.dag import DAG` | Moved module | `from airflow.sdk import DAG` |
| `XCom pickle serialization error` | Pickle removed | Ensure all XCom values are JSON-serializable |
| `TypeError: PythonOperator.provide_context is not supported` | Removed param | Delete `provide_context=True` |

---

## 1. DAG parameters

| 2.10 | 3.1 | Notes |
|---|---|---|
| `schedule_interval="@daily"` | `schedule="@daily"` | Accepts cron, timedelta, timetable, Asset, or None |
| `schedule_interval=timedelta(hours=1)` | `schedule=timedelta(hours=1)` | |
| `schedule_interval=None` | `schedule=None` | No automatic scheduling |
| `concurrency=4` | `max_active_tasks=4` | |
| `fail_stop=True` | `fail_fast=True` | |
| `default_view="graph"` | *(removed)* | |
| `orientation="LR"` | *(removed)* | |
| `sla_miss_callback=fn` | `deadline=DeadlineAlert(timedelta(...))` | See 06_deadline.py |
| `catchup=True` (was default) | `catchup=False` (new default) | |
| `timetable=MyTimetable()` | `schedule=MyTimetable()` | Pass timetable to `schedule=` |

## 2. Operator / task parameters

| 2.10 | 3.1 | Notes |
|---|---|---|
| `task_concurrency=N` | `max_active_tis_per_dag=N` | (Already renamed in 2.x; errors in 3.x) |
| `sla=timedelta(...)` | *(removed)* | Use DAG-level `deadline=` instead |
| `provide_context=True` | *(removed)* | Context always injected in 3.x |
| `email_on_failure=True` | Use `SmtpNotifier` | Emits `RemovedInAirflow4Warning` |
| `email_on_retry=True` | Use `SmtpNotifier` | Emits `RemovedInAirflow4Warning` |
| Unknown `**kwargs` → warning | Unknown `**kwargs` → `TypeError` | All extra kwargs must be removed |

## 3. Import path changes

All old import paths work as deprecation shims in 3.x (until noted). Migrate proactively.

| 2.10 import | 3.1 canonical import |
|---|---|
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.models.dag import DAG` | `from airflow.sdk import DAG` |
| `from airflow.decorators import dag, task, task_group` | `from airflow.sdk import dag, task, task_group` |
| `from airflow.decorators import setup, teardown` | `from airflow.sdk import setup, teardown` |
| `from airflow.models.baseoperator import BaseOperator` | `from airflow.sdk import BaseOperator` |
| `from airflow.sensors.base import BaseSensorOperator` | `from airflow.sdk import BaseSensorOperator` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `from airflow.operators.python import BranchPythonOperator` | `from airflow.providers.standard.operators.python import BranchPythonOperator` |
| `from airflow.operators.python import ShortCircuitOperator` | `from airflow.providers.standard.operators.python import ShortCircuitOperator` |
| `from airflow.operators.python import PythonVirtualenvOperator` | `from airflow.providers.standard.operators.python import PythonVirtualenvOperator` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.empty import EmptyOperator` | `from airflow.providers.standard.operators.empty import EmptyOperator` |
| `from airflow.operators.latest_only import LatestOnlyOperator` | `from airflow.providers.standard.operators.latest_only import LatestOnlyOperator` |
| `from airflow.operators.datetime import BranchDateTimeOperator` | `from airflow.providers.standard.operators.datetime import BranchDateTimeOperator` |
| `from airflow.operators.trigger_dagrun import TriggerDagRunOperator` | `from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator` |
| `from airflow.operators.email import EmailOperator` | `from airflow.providers.smtp.operators.smtp import EmailOperator` |
| `from airflow.operators.subdag import SubDagOperator` | **REMOVED** — use `TaskGroup` |
| `from airflow.sensors.time_delta import TimeDeltaSensor` | `from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor` |
| `from airflow.sensors.time_delta import TimeDeltaSensorAsync` | Use `TimeDeltaSensor(deferrable=True)` |
| `from airflow.sensors.external_task import ExternalTaskSensor` | `from airflow.providers.standard.sensors.external_task import ExternalTaskSensor` |
| `from airflow.sensors.filesystem import FileSensor` | `from airflow.providers.standard.sensors.filesystem import FileSensor` |
| `from airflow.sensors.date_time import DateTimeSensor` | `from airflow.providers.standard.sensors.date_time import DateTimeSensor` |
| `from airflow.datasets import Dataset` | `from airflow.sdk import Asset` |
| `from airflow.datasets import DatasetAll` | `from airflow.sdk import AssetAll` |
| `from airflow.datasets import DatasetAny` | `from airflow.sdk import AssetAny` |
| `from airflow.datasets import DatasetAlias` | `from airflow.sdk import AssetAlias` |
| `from airflow.timetables.datasets import DatasetOrTimeSchedule` | `from airflow.timetables.assets import AssetOrTimeSchedule` |
| `from airflow.models.xcom import BaseXCom` | `from airflow.models.xcom import XComModel` |
| `from airflow.models import Variable` | `from airflow.sdk import Variable` |

## 4. Context variables (templates and **context kwargs)

| 2.10 | 3.1 | Notes |
|---|---|---|
| `{{ execution_date }}` | `{{ logical_date }}` | Same moment in time |
| `context["execution_date"]` | `context["logical_date"]` | |
| `{{ next_execution_date }}` | `{{ data_interval_end }}` | |
| `context["next_execution_date"]` | `context["data_interval_end"]` | |
| `context["prev_execution_date"]` | *(no equivalent)* | |
| `{{ next_ds }}` | `{{ data_interval_end \| ds }}` | |
| `{{ prev_ds }}` | *(no equivalent)* | |
| `{{ next_ds_nodash }}` | `{{ data_interval_end \| ds_nodash }}` | |
| `context["triggering_dataset_events"]` | `context["triggering_asset_events"]` | |
| `context["run_id"]` | `context["run_id"]` | Unchanged |
| `context["ds"]` | `context["ds"]` | Unchanged (= `logical_date \| ds`) |

## 5. Dataset → Asset rename

| 2.10 | 3.1 | Shim until |
|---|---|---|
| `Dataset(uri=...)` | `Asset(uri=...)` | 3.2 |
| `DatasetAll(...)` | `AssetAll(...)` | 3.2 |
| `DatasetAny(...)` | `AssetAny(...)` | 3.2 |
| `DatasetAlias(...)` | `AssetAlias(...)` | 3.2 |
| `DatasetOrTimeSchedule` | `AssetOrTimeSchedule` | 3.2 |
| `triggering_dataset_events` | `triggering_asset_events` | Now |

New Asset fields (not in Dataset):
- `name=` — human-readable identifier (can be used alone instead of URI)
- `group=` — classification
- `watchers=` — list of `AssetWatcher` for event-driven processing

## 6. XCom

| 2.10 | 3.1 |
|---|---|
| JSON and pickle both supported | JSON only (pickle removed, migration `0049`) |
| `BaseXCom` class name | `XComModel` class name |
| `xcom_pull(..., execution_date=dt)` | `xcom_pull(..., run_id=run_id)` |
| Any Python object can be pushed | Must be JSON-serializable |

**Types that break in 3.x:** `set`, `tuple` (returns as `list`), arbitrary class instances, `datetime` (use `.isoformat()`), `Decimal`, `bytes`, `numpy` arrays.

**Fix:** Convert to JSON-friendly types before pushing: `list(my_set)`, `str(my_dt)`, etc.

## 7. Removed features

| Feature | Replacement |
|---|---|
| SubDAG / `SubDagOperator` | `TaskGroup` |
| `SlaMiss` table and per-task `sla=` | `deadline=DeadlineAlert(...)` on DAG |
| `sla_miss_callback` | `DeadlineAlert(callback=fn)` |
| `DagPickle` / DAG pickling | Serialized DAGs (JSON) |
| `TaskFail` table | `TaskInstanceHistory` table |
| `DagRun.execution_date` column | `DagRun.logical_date` column |
| `external_trigger: bool` on DagRun | `triggered_by` enum field |
| XCom pickle column | JSON-only XCom |

## 8. New in 3.1 only (not in 2.10)

| Feature | Import / Usage |
|---|---|
| `@task.run_if(condition_fn)` | `from airflow.sdk import task` |
| `@task.skip_if(condition_fn)` | `from airflow.sdk import task` |
| `@asset` decorator | `from airflow.sdk.definitions.asset.decorators import asset` |
| `DeadlineAlert` | `from airflow.timetables.deadline import DeadlineAlert` |
| `Asset.name` field | `Asset(name="my-asset")` |
| `Asset.group` field | `Asset(group="my-group")` |
| `Model` class (ML assets) | `from airflow.sdk import Model` |
| `DagVersion` model | Internal; accessed via TI/DagRun |
| `DagBundle` concept | `[dag_processor]` config section |
| `TaskInstanceHistory` model | Replaces `TaskFail` |
| `DagRun.triggered_by` enum | `CLI`, `REST_API`, `SCHEDULER`, `ASSET`, etc. |
| `DagRun.run_after` field | Earliest scheduled start |

## 9. Example DAGs in this folder

```
dags_2_10_3rc2/
  01_basic_dag.py              — DAG params, schedule_interval, context vars
  02_taskflow_api.py           — @dag/@task, decorators, context in 2.x
  03_datasets.py               — Dataset, DatasetAll/Any/Alias, triggers
  04_xcom.py                   — XCom push/pull including pickle (2.x only)
  05_subdag_and_branches.py    — SubDagOperator, BranchPythonOperator
  06_sla.py                    — sla_miss_callback, per-task sla=
  07_sensors.py                — BaseSensorOperator, TimeDeltaSensorAsync
  08_dynamic_task_mapping.py   — expand/partial/expand_kwargs
  09_variables_connections.py  — Variable.get, BaseHook.get_connection
  10_callbacks_notifications.py — callbacks, email_on_failure, EmailOperator

dags_3_1_7rc2/
  01_basic_dag.py              — schedule=, max_active_tasks, logical_date
  02_taskflow_api.py           — airflow.sdk decorators, run_if/skip_if
  03_assets.py                 — Asset, AssetAll/Any/Alias, @asset decorator
  04_xcom.py                   — JSON-only XCom, XComModel, run_id param
  05_task_groups_and_branches.py — TaskGroup (replaces SubDag), branches
  06_deadline.py               — DeadlineAlert (replaces SLA)
  07_sensors.py                — providers.standard sensors, deferrable=True
  08_dynamic_task_mapping.py   — expand/partial (unchanged, different imports)
  09_variables_connections.py  — Variable/Connection via Execution API
  10_callbacks_notifications.py — SmtpNotifier, EmailOperator from providers.smtp
```
