# coding=utf-8
# Copyright 2022 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Airflow DAG for notification of completion with the summary of Content API for Shopping results.

This DAG relies on multiple Airflow variables
https://airflow.apache.org/concepts.html#variables

* GCP_PROJECT_ID - GCP project ID.
* QUEUE_LOCATION - the location of Cloud Tasks queue.
* QUEUE_NAME - the name of Cloud Tasks queue.
* TRY_COUNT_LIMIT - maximum number of times checking the status of Cloud Tasks.
* MONITOR_DATASET_ID - monitor BigQuery dataset ID.
* MONITOR_TABLE_ID - monitor BigQuery table ID.
* LAST_PROCESS_RESULT_QUERY_FILE_PATH - file path of the query to get the last
result.
* DESTINATION_PUBSUB_TOPIC - Cloud Pub/Sub topic name.
* FEED_DATASET_ID - feed BigQuery dataset ID.
* ITEMS_TABLE_ID - items BigQuery table ID.
"""

import datetime

from airflow import models
from airflow.contrib.operators import bigquery_operator
from frozendict import frozendict

from sfo_plugin.operators import bq_to_pubsub_operator
from sfo_plugin.operators import clean_up_operator
from sfo_plugin.operators import wait_for_completion_operator

# DAG configs
_WAIT_FOR_COMPLETION_TASK = 'wait_for_completion'
_UPDATE_EXPIRATION_TRACKING = 'update_expiration_tracking'
_CLEAN_UP_TASK = 'clean_up'
_TRIGGER_REPORTING_TASK = 'get_run_results_and_trigger_reporting'
# Airflow variables
_DAG_ID = models.Variable.get('DAG_ID')
_PROJECT_ID = models.Variable.get('GCP_PROJECT_ID')
_QUEUE_LOCATION = models.Variable.get('QUEUE_LOCATION')
_QUEUE_NAME = models.Variable.get('QUEUE_NAME')
_TRY_COUNT_LIMIT = int(models.Variable.get('TRY_COUNT_LIMIT'))
_MONITOR_DATASET_ID = models.Variable.get('MONITOR_DATASET_ID')
_MONITOR_TABLE_ID = models.Variable.get('MONITOR_TABLE_ID')
_RESULT_QUERY_FILE = models.Variable.get('LAST_PROCESS_RESULT_QUERY_FILE_PATH')
_TOPIC_NAME = models.Variable.get('DESTINATION_PUBSUB_TOPIC')
_TIMEZONE_UTC_OFFSET = models.Variable.get('TIMEZONE_UTC_OFFSET')
_FEED_DATASET_ID = models.Variable.get('FEED_DATASET_ID')
_ITEMS_TABLE_ID = models.Variable.get('ITEMS_TABLE_ID')
_EXPIRATION_TRACKING_TABLE_ID = models.Variable.get(
    'EXPIRATION_TRACKING_TABLE_ID')
_ITEM_RESULTS_TABLE_ID = models.Variable.get('ITEM_RESULTS_TABLE_ID')
_LOCK_BUCKET = models.Variable.get('LOCK_BUCKET')
# File paths
_SERVICE_ACCOUNT = '/home/airflow/gcs/data/config/service_account.json'
_UPDATE_ITEMS_EXPIRATION_TRACKING_QUERY = 'queries/update_items_expiration_tracking.sql'

_DEFAULT_DAG_ARGS = frozendict({
    # Since this dag is not scheduled, setting start_date now.
    'start_date': datetime.datetime.now(),
    'retries': 5,
    'project_id': _PROJECT_ID
})

dag = models.DAG(
    dag_id=_DAG_ID,
    default_args=dict(_DEFAULT_DAG_ARGS),
    schedule_interval=None)

_wait_for_completion = wait_for_completion_operator.WaitForCompletionOperator(
    task_id=_WAIT_FOR_COMPLETION_TASK,
    dag=dag,
    project_id=_PROJECT_ID,
    queue_location=_QUEUE_LOCATION,
    queue_name=_QUEUE_NAME,
    service_account_path=_SERVICE_ACCOUNT,
    try_count_limit=_TRY_COUNT_LIMIT)

_update_expiration_tracking = bigquery_operator.BigQueryOperator(
    task_id=_UPDATE_EXPIRATION_TRACKING,
    dag=dag,
    sql=_UPDATE_ITEMS_EXPIRATION_TRACKING_QUERY,
    use_legacy_sql=False,
    params={
        'timezone': _TIMEZONE_UTC_OFFSET,
        'feed_dataset_id': _FEED_DATASET_ID,
        'expiration_tracking_table_id': _EXPIRATION_TRACKING_TABLE_ID,
        'monitor_dataset_id': _MONITOR_DATASET_ID,
        'item_results_table_id': _ITEM_RESULTS_TABLE_ID
    })

_bq_to_pubsub = bq_to_pubsub_operator.GetRunResultsAndTriggerReportingOperator(
    task_id=_TRIGGER_REPORTING_TASK,
    dag=dag,
    project_id=_PROJECT_ID,
    dataset_id=_MONITOR_DATASET_ID,
    table_id=_MONITOR_TABLE_ID,
    query_file_path=_RESULT_QUERY_FILE,
    topic_name=_TOPIC_NAME)

_clean_up = clean_up_operator.CleanUpOperator(
    task_id=_CLEAN_UP_TASK,
    dag=dag,
    dataset_id=_FEED_DATASET_ID,
    table_id=_ITEMS_TABLE_ID,
    bucket_id=_LOCK_BUCKET)

_wait_for_completion >> _update_expiration_tracking >> _bq_to_pubsub >> _clean_up  
