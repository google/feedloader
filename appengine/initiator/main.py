# coding=utf-8
# Copyright 2023 Google LLC.
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

"""Initiator module that sends jobs to Task Queue when triggered.

This module provides a handler to which Cloud Pub/Sub will push a message after
completing processing items. After receiving a message, it makes batch jobs and
sends them to Task Queue.
"""

import datetime
from distutils import util
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

LOCAL_SUFFIX = '-local'

_SERVICE_ACCOUNT = './config/service_account.json'
_QUEUE_NAME = 'processing-items'
_QUEUE_NAME_LOCAL = _QUEUE_NAME + LOCAL_SUFFIX
_BATCH_SIZE = 1000

_CHANNEL_LOCAL = 'local'
_CHANNEL_ONLINE = 'online'

_DATASET_ID_PROCESSING_FEED_DATA = 'processing_feed_data'
_DATASET_ID_PROCESSING_FEED_DATA_LOCAL = 'processing_feed_data_local'
_DATASET_ID_FEED_DATA = 'feed_data'
_DATASET_ID_FEED_DATA_LOCAL = 'feed_data_local'
_TABLE_ID_ITEMS = 'items'
_QUERY_FILEPATH_FOR_UPSERT = 'queries/items_to_upsert.sql'
_QUERY_FILEPATH_FOR_DELETE = 'queries/items_to_delete.sql'
_QUERY_FILEPATH_FOR_PREVENT_EXPIRING = 'queries/items_to_prevent_expiring.sql'
_MAILER_TOPIC_NAME = 'mailer-trigger'

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

app = flask.Flask(__name__)


@app.route('/start', methods=['POST'])
def start() -> Tuple[str, http.HTTPStatus]:
  """Pushes tasks to Cloud Tasks when receiving a task from Cloud Tasks.

  The request body must be of a format like:
  {
    'deleteCount': 1000,
    'expiringCount': 2000,
    'upsertCount': 3000,
  }

  The response is an HTTP response with a message.
  - 200:
    description: the request is successfully processed.
  - 400:
    description: the request is invalid and failed to be processed.

  Returns:
    message and HTTP status code.
  """
  logging_client = cloud_logging.Client()
  logging_client.setup_logging(log_level=logging.DEBUG)

  queue_name = flask.request.headers.get('X-Appengine-Queuename')
  logging.info('Queue name of the incoming request is %s.', queue_name)

  local_inventory_feed_enabled = True if 'local' in queue_name else False

  try:
    request_body = json.loads(flask.request.data)
  except TypeError:
    _cleanup(local_inventory_feed_enabled)
    return 'Request body is not a string.', http.HTTPStatus.BAD_REQUEST
  except ValueError:
    _cleanup(local_inventory_feed_enabled)
    return 'Request body is not in JSON format.', http.HTTPStatus.BAD_REQUEST
  logging.info('Request body: %s', request_body)
  try:
    task = initiator_task.InitiatorTask.from_json(request_body)
  except ValueError as error:
    logging.error('Error parsing the task JSON: %s', error)
    _cleanup(local_inventory_feed_enabled)
    return 'Message is invalid.', http.HTTPStatus.BAD_REQUEST
  logging.info(
      (
          'Initiator received a message. upsert_count: %d, delete_count: %d, '
          'expiring_count: %d.'
      ),
      task.upsert_count,
      task.delete_count,
      task.expiring_count,
  )
  timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
  any_task_started = False
  try:
    if task.upsert_count > 0:
      _create_processing_table(
          _TABLE_SUFFIX_UPSERT,
          _QUERY_FILEPATH_FOR_UPSERT,
          timestamp,
          local_inventory_feed_enabled,
      )
      _create_tasks_in_cloud_tasks(
          _TARGET_URL_INSERT,
          task.upsert_count,
          timestamp,
          local_inventory_feed_enabled,
      )
      any_task_started = True
    if task.delete_count > 0:
      _create_processing_table(
          _TABLE_SUFFIX_DELETE,
          _QUERY_FILEPATH_FOR_DELETE,
          timestamp,
          local_inventory_feed_enabled,
      )
      _create_tasks_in_cloud_tasks(
          _TARGET_URL_DELETE,
          task.delete_count,
          timestamp,
          local_inventory_feed_enabled,
      )
      any_task_started = True
    if task.expiring_count > 0 and not local_inventory_feed_enabled:
      _create_processing_table(
          _TABLE_SUFFIX_PREVENT_EXPIRING,
          _QUERY_FILEPATH_FOR_PREVENT_EXPIRING,
          timestamp,
      )
      _create_tasks_in_cloud_tasks(
          _TARGET_URL_PREVENT_EXPIRING, task.expiring_count, timestamp
      )
      any_task_started = True
  except TypeError:
    logging.exception('An invalid numeric value was provided.')
    _cleanup(local_inventory_feed_enabled)
    return (
        (
            'An invalid numeric value was provided. Upsert count: '
            f'{task.upsert_count}. Delete count: {task.delete_count}. '
            f'Expiring count: {task.expiring_count}'
        ),
        http.HTTPStatus.BAD_REQUEST,
    )
  except cloud_exceptions.GoogleCloudError as gcp_error:
    logging.exception('GCP error raised.')
    _cleanup(local_inventory_feed_enabled)
    if gcp_error.code:
      response_code = gcp_error.code
    else:
      response_code = http.HTTPStatus.INTERNAL_SERVER_ERROR
    return 'GCP API returned an error.', response_code
  except auth_exceptions.GoogleAuthError:
    logging.exception('Authorization error raised due to service account.')
    _cleanup(local_inventory_feed_enabled)
    return 'Authorization failed.', http.HTTPStatus.INTERNAL_SERVER_ERROR
  # Trigger monitoring cloud composer only when items are sent.
  if any_task_started:
    _trigger_monitoring_cloud_composer()
  else:
    # No processing required, so just clean up and send an email.
    _cleanup(local_inventory_feed_enabled)
    _trigger_mailer_for_nothing_processed(local_inventory_feed_enabled)
  logging.info('Initiator has successfully finished!')
  return 'OK', http.HTTPStatus.OK


def _create_processing_table(
    table_suffix: str,
    query_filepath: str,
    timestamp: str,
    local_inventory_feed_enabled: bool = False,
) -> None:
  """Creates a processing table to allow uploader to load items from it.

  Args:
    table_suffix: name of the BigQuery table suffix.
    query_filepath: filepath to a query file.
    timestamp: timestamp to identify the run.
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.
  """
  project_id = _load_environment_variable('PROJECT_ID')
  if local_inventory_feed_enabled:
    feed_data_dataset_id = _DATASET_ID_FEED_DATA_LOCAL
    processing_feed_data_dataset_id = _DATASET_ID_PROCESSING_FEED_DATA_LOCAL
  else:
    feed_data_dataset_id = _DATASET_ID_FEED_DATA
    processing_feed_data_dataset_id = _DATASET_ID_PROCESSING_FEED_DATA
  try:
    query = bigquery_client.generate_query_string(
        query_filepath, project_id, feed_data_dataset_id
    )
  except IOError as io_error:
    logging.exception(io_error.message)
  else:
    table_id = f'process_items_to_{table_suffix}_{timestamp}'
    bq_client = bigquery_client.BigQueryClient.from_service_account_json(
        _SERVICE_ACCOUNT, processing_feed_data_dataset_id, table_id
    )
    bq_client.initialize_dataset_and_table(query)


def _create_tasks_in_cloud_tasks(
    target_url: str,
    items_count: int,
    timestamp: str,
    local_inventory_feed_enabled: bool = False,
) -> None:
  """Creates tasks in Cloud Tasks to execute uploader.

  Args:
    target_url: target url of uploader.
    items_count: number of items to be processed.
    timestamp: timestamp to identify the run.
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.

  Raises:
    LocalInventoryFeedEnabledButLIADisabledError: Find an inconsistency between
      local_inventory_feed_enabled and use_lia.
  """
  project_id = _load_environment_variable('PROJECT_ID')
  location = _load_environment_variable('REGION')
  use_local_inventory_ads = (
      _load_environment_variable('USE_LOCAL_INVENTORY_ADS'))
  if local_inventory_feed_enabled:
    queue_name = _QUEUE_NAME_LOCAL
  else:
    queue_name = _QUEUE_NAME
  cloudtasks_client = tasks_client.TasksClient.from_service_account_json(
      _SERVICE_ACCOUNT,
      url=target_url,
      project_id=project_id,
      location=location,
      queue_name=queue_name,
  )

  try:
    use_lia = bool(util.strtobool(use_local_inventory_ads))
  except ValueError:
    use_lia = False

  if use_lia:
    cloudtasks_client.push_tasks(
        total_items=items_count,
        batch_size=_BATCH_SIZE,
        timestamp=timestamp,
        channel=_CHANNEL_LOCAL)
    if not local_inventory_feed_enabled:
      cloudtasks_client.push_tasks(
          total_items=items_count,
          batch_size=_BATCH_SIZE,
          timestamp=timestamp,
          channel=_CHANNEL_ONLINE,
      )
  else:
    if local_inventory_feed_enabled:
      raise LocalInventoryFeedEnabledButLIADisabledError(
          'Find an inconsistency between local_inventory_feed_enabled and '
          'use_lia. local_inventory_feed_enabled is True but use_lia is false.'
      )
    cloudtasks_client.push_tasks(
        total_items=items_count,
        batch_size=_BATCH_SIZE,
        timestamp=timestamp,
        channel=_CHANNEL_ONLINE)


def _trigger_monitoring_cloud_composer() -> None:
  """Triggers the monitoring application."""
  trigger_completion_bucket = (
      _load_environment_variable('TRIGGER_COMPLETION_BUCKET'))
  gcs_client = storage_client.StorageClient.from_service_account_json(
      _SERVICE_ACCOUNT, trigger_completion_bucket)
  gcs_client.upload_eof()


def _cleanup(local_inventory_feed_enabled: bool) -> None:
  """Cleans up resources for the current run to allow another run to start.

  Args:
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.
  """
  _delete_items_table(local_inventory_feed_enabled)
  _delete_eof_lock(local_inventory_feed_enabled)


def _delete_items_table(local_inventory_feed_enabled: bool) -> None:
  """Deletes items table to allow the next run.

  Args:
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.
  """
  if local_inventory_feed_enabled:
    dataset_id = _DATASET_ID_FEED_DATA_LOCAL
  else:
    dataset_id = _DATASET_ID_FEED_DATA
  bq_client = bigquery_client.BigQueryClient.from_service_account_json(
      _SERVICE_ACCOUNT, dataset_id, _TABLE_ID_ITEMS
  )
  bq_client.delete_table()


def _delete_eof_lock(local_inventory_feed_enabled: bool) -> None:
  """Deletes EOF.lock file to allow the next run.

  Args:
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.
  """
  lock_bucket = _load_environment_variable('LOCK_BUCKET')
  if local_inventory_feed_enabled:
    lock_bucket += LOCAL_SUFFIX
  gcs_client = storage_client.StorageClient.from_service_account_json(
      _SERVICE_ACCOUNT, lock_bucket)
  gcs_client.delete_eof_lock()


def _trigger_mailer_for_nothing_processed(
    local_inventory_feed_enabled: bool) -> None:
  """Sends a completion email showing for the case when there was no diff."""
  project_id = _load_environment_variable('PROJECT_ID')
  pubsub_publisher = pubsub_client.PubSubClient.from_service_account_json(
      _SERVICE_ACCOUNT)
  operation_counts_dict = {
      operation: operation_counts.OperationCounts(operation, 0, 0, 0)
      for operation in OPERATIONS
  }
  pubsub_publisher.trigger_result_email(project_id,
                                        _MAILER_TOPIC_NAME,
                                        operation_counts_dict,
                                        local_inventory_feed_enabled)


def _load_environment_variable(key: str) -> str:
  """Helper that loads an environment variable with the matching key."""
  return os.environ.get(key, '')


class LocalInventoryFeedEnabledButLIADisabledError(Exception):
  """Raised when local inventory feed is enabled but LIA is disabled."""


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
