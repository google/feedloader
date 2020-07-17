# coding=utf-8
# Copyright 2021 Google LLC.
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

# python3
"""Initiator module that sends jobs to Task Queue when triggered.

This module provides a handler to which Cloud Pub/Sub will push a message after
completing processing items. After receiving a message, it makes batch jobs and
sends them to Task Queue.
"""

import datetime
import http
import json
import logging
import os
from typing import Tuple

import flask
from google.auth import exceptions as auth_exceptions
from google.cloud import exceptions as cloud_exceptions
from google.cloud import logging as cloud_logging

import bigquery_client
from models import operation_counts
from models import initiator_task

import pubsub_client
import storage_client
import tasks_client

_SERVICE_ACCOUNT = './config/service_account.json'
_PROJECT_ID = os.environ['PROJECT_ID']
_LOCATION = os.environ['REGION']
_QUEUE_NAME = 'processing-items'
_BATCH_SIZE = 1000
_TRIGGER_COMPLETION_BUCKET = os.environ['TRIGGER_COMPLETION_BUCKET']
_LOCK_BUCKET = os.environ['LOCK_BUCKET']
_DATASET_ID_PROCESSING_FEED_DATA = 'processing_feed_data'
_DATASET_ID_FEED_DATA = 'feed_data'
_TABLE_ID_ITEMS = 'items'
_QUERY_FILEPATH_FOR_UPSERT = 'queries/items_to_upsert.sql'
_QUERY_FILEPATH_FOR_DELETE = 'queries/items_to_delete.sql'
_QUERY_FILEPATH_FOR_PREVENT_EXPIRING = 'queries/items_to_prevent_expiring.sql'
_MAILER_TOPIC_NAME = 'mailer-trigger'

_API_METHOD_INSERT = 'insert'
_API_METHOD_DELETE = 'delete'

_TABLE_SUFFIX_UPSERT = 'upsert'
_TABLE_SUFFIX_DELETE = 'delete'
_TABLE_SUFFIX_PREVENT_EXPIRING = 'prevent_expiring'

OPERATION_UPSERT = 'upsert'
OPERATION_DELETE = 'delete'
OPERATION_EXPIRING = 'expiring'
OPERATIONS = (OPERATION_UPSERT, OPERATION_DELETE, OPERATION_EXPIRING)

_TARGET_URL_INSERT = '/insert_items'
_TARGET_URL_DELETE = '/delete_items'
_TARGET_URL_PREVENT_EXPIRING = '/prevent_expiring_items'

logging_client = cloud_logging.Client()
logging_client.setup_logging(log_level=logging.DEBUG)

app = flask.Flask(__name__)


@app.route('/start', methods=['POST'])
def start() -> Tuple[str, http.HTTPStatus]:
  """Pushes tasks to Cloud Tasks when receiving a task from Cloud Tasks.

  The request body must be of a format like:
  {
    'deleteCount': 1,
    'expiringCount': 2,
    'upsertCount': 3,
  }

  The response is an HTTP response with a message.
  - 200:
    description: the request is successfully processed.
  - 400:
    description: the request is invalid and failed to be processed.

  Returns:
    message and HTTP status code.
  """
  try:
    request_body = json.loads(flask.request.data)
  except TypeError:
    _cleanup()
    return 'Request body is not a string.', http.HTTPStatus.BAD_REQUEST
  except ValueError:
    _cleanup()
    return 'Request body is not in JSON format.', http.HTTPStatus.BAD_REQUEST
  logging.info('Request body: %s', request_body)
  try:
    task = initiator_task.InitiatorTask.from_json(request_body)
  except ValueError as error:
    logging.error('Error parsing the task JSON: %s', error)
    _cleanup()
    return 'Message is invalid.', http.HTTPStatus.BAD_REQUEST
  logging.info(
      'Initiator received a message. upsert_count: %d, delete_count: %d, expiring_count: %d.',
      task.upsert_count, task.delete_count, task.expiring_count)
  timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
  any_task_started = False
  try:
    if task.upsert_count > 0:
      _create_processing_table(_TABLE_SUFFIX_UPSERT, _QUERY_FILEPATH_FOR_UPSERT,
                               timestamp)
      _create_tasks_in_cloud_tasks(_TARGET_URL_INSERT, task.upsert_count,
                                   timestamp)
      any_task_started = True
    if task.delete_count > 0:
      _create_processing_table(_TABLE_SUFFIX_DELETE, _QUERY_FILEPATH_FOR_DELETE,
                               timestamp)
      _create_tasks_in_cloud_tasks(_TARGET_URL_DELETE, task.delete_count,
                                   timestamp)
      any_task_started = True
    if task.expiring_count > 0:
      _create_processing_table(_TABLE_SUFFIX_PREVENT_EXPIRING,
                               _QUERY_FILEPATH_FOR_PREVENT_EXPIRING, timestamp)
      _create_tasks_in_cloud_tasks(_TARGET_URL_PREVENT_EXPIRING,
                                   task.expiring_count, timestamp)
      any_task_started = True
  except cloud_exceptions.GoogleCloudError as gcp_error:
    logging.exception('GCP error raised.')
    _cleanup()
    response_code = gcp_error.code if gcp_error.code else http.HTTPStatus.INTERNAL_SERVER_ERROR
    return 'GCP API returned an error.', response_code
  except auth_exceptions.GoogleAuthError:
    logging.exception('Authorization error raised due to service account.')
    _cleanup()
    return 'Authorization failed.', http.HTTPStatus.INTERNAL_SERVER_ERROR
  # Trigger monitoring cloud composer only when items are sent.
  if any_task_started:
    _trigger_monitoring_cloud_composer()
  else:
    # No processing required, so just clean up and send an email.
    _cleanup()
    _trigger_mailer_for_nothing_processed()
  logging.info('Initiator has successfully finished!')
  return 'OK', http.HTTPStatus.OK


def _create_processing_table(table_suffix: str, query_filepath: str,
                             timestamp: str) -> None:
  """Creates a processing table to allow uploader to load items from it.

  Args:
    table_suffix: name of the BigQuery table suffix.
    query_filepath: filepath to a query file.
    timestamp: timestamp to identify the run.
  """
  try:
    query = bigquery_client.generate_query_string(query_filepath, _PROJECT_ID)
  except IOError as io_error:
    logging.exception(io_error.message)
  else:
    table_id = f'process_items_to_{table_suffix}_{timestamp}'
    bq_client = bigquery_client.BigQueryClient.from_service_account_json(
        _SERVICE_ACCOUNT, _DATASET_ID_PROCESSING_FEED_DATA, table_id)
    bq_client.initialize_dataset_and_table(query)


def _create_tasks_in_cloud_tasks(target_url: str, items_count: int,
                                 timestamp: str) -> None:
  """Creates tasks in Cloud Tasks to execute uploader.

  Args:
    target_url: target url of uploader.
    items_count: number of items to be processed.
    timestamp: timestamp to identify the run.
  """
  ct_client = tasks_client.TasksClient.from_service_account_json(
      _SERVICE_ACCOUNT,
      url=target_url,
      project_id=_PROJECT_ID,
      location=_LOCATION,
      queue_name=_QUEUE_NAME)
  ct_client.push_tasks(
      total_items=items_count, batch_size=_BATCH_SIZE, timestamp=timestamp)


def _trigger_monitoring_cloud_composer() -> None:
  """Triggers the monitoring application."""
  gcs_client = storage_client.StorageClient.from_service_account_json(
      _SERVICE_ACCOUNT, _TRIGGER_COMPLETION_BUCKET)
  gcs_client.upload_eof()


def _cleanup() -> None:
  """Cleans up resources for the current run to allow another run to start."""
  _delete_items_table()
  _delete_eof_lock()


def _delete_items_table() -> None:
  """Deletes items table to allow the next run."""
  bq_client = bigquery_client.BigQueryClient.from_service_account_json(
      _SERVICE_ACCOUNT, _DATASET_ID_FEED_DATA, _TABLE_ID_ITEMS)
  bq_client.delete_table()


def _delete_eof_lock() -> None:
  """Deletes EOF.lock file to allow the next run."""
  gcs_client = storage_client.StorageClient.from_service_account_json(
      _SERVICE_ACCOUNT, _LOCK_BUCKET)
  gcs_client.delete_eof_lock()


def _trigger_mailer_for_nothing_processed() -> None:
  """Sends a completion email showing 0 upsert/deletion/expiring calls (sent when no diff)."""
  pubsub_publisher = pubsub_client.PubSubClient.from_service_account_json(
      _SERVICE_ACCOUNT)
  operation_counts_dict = {
      operation: operation_counts.OperationCounts(operation, 0, 0, 0)
      for operation in OPERATIONS
  }
  pubsub_publisher.trigger_result_email(_PROJECT_ID, _MAILER_TOPIC_NAME,
                                        operation_counts_dict)


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
