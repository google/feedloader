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

"""Uploader module that handles batch jobs sent from Task Queue.

This module receives batch jobs from TaskQueue. For each job, the module loads
data from BigQuery and sends it to Merchant Center.
"""

import http
import json
import logging
import socket
from typing import List, Tuple

import flask
from google.cloud import bigquery
from google.cloud import logging as cloud_logging
from googleapiclient import errors

import batch_creator
import bigquery_client
import constants
import content_api_client
import result_recorder
import shoptimizer_client
from models import failure
from models import process_result
from models import upload_task

app = flask.Flask(__name__)

_logging_client = cloud_logging.Client()
_logging_client.setup_logging(log_level=logging.DEBUG)

_SHOPTIMIZER_CONFIG_FILE_PATH = 'config/shoptimizer_config.json'

OPERATION_TO_METHOD = {
    constants.Operation.UPSERT: constants.Method.INSERT,
    constants.Operation.DELETE: constants.Method.DELETE,
    constants.Operation.PREVENT_EXPIRING: constants.Method.INSERT
}

# Used to check if this is the last retry for alerting purposes.
# Should match task_retry_limit in appengine/initiator/queue.yaml.
TASK_RETRY_LIMIT = 5


@app.route('/insert_items', methods=['POST'])
def run_insert_process() -> Tuple[str, http.HTTPStatus]:
  """Handles uploading tasks pushed from Task Queue."""
  return _run_process(constants.Operation.UPSERT)


@app.route('/delete_items', methods=['POST'])
def run_delete_process() -> Tuple[str, http.HTTPStatus]:
  """Handles deleting tasks pushed from Task Queue."""
  return _run_process(constants.Operation.DELETE)


@app.route('/prevent_expiring_items', methods=['POST'])
def run_prevent_expiring_process() -> Tuple[str, http.HTTPStatus]:
  """Handles prevent expiring tasks pushed from Task Queue."""
  return _run_process(constants.Operation.PREVENT_EXPIRING)


def _run_process(operation: constants.Operation) -> Tuple[str, http.HTTPStatus]:
  """Handles tasks pushed from Task Queue.

  When tasks are enqueued to Task Queue by initiator, this method will be
  called. It extracts necessary information from a Task Queue message. The
  following processes are executed in this function:
  - Loading items to process from BigQuery.
  - Converts items into a batch that can be sent to Content API for Shopping.
  - Sending items to Content API for Shopping (Merchant Center).
  - Records the results of the Content API for Shopping call.

  Args:
    operation: Type of operation to perform on the items.

  Returns:
    The result of HTTP request.
  """
  request_body = json.loads(flask.request.data.decode('utf-8'))
  task = upload_task.UploadTask.from_json(request_body)

  if task.batch_size == 0:
    return 'OK', http.HTTPStatus.OK

  batch_number = int(task.start_index / task.batch_size) + 1
  logging.info(
      '%s started. Batch #%d info: start_index: %d, batch_size: %d,'
      'initiation timestamp: %s', operation.value, batch_number,
      task.start_index, task.batch_size, task.timestamp)

  try:
    items = _load_items_from_bigquery(operation, task)
  except errors.HttpError:
    return 'Error loading items from BigQuery', http.HTTPStatus.INTERNAL_SERVER_ERROR

  result = process_result.ProcessResult([], [], [])
  try:
    if not items:
      logging.error(
          'Batch #%d, operation %s: 0 items loaded from BigQuery so batch not sent to Content API. Start_index: %d, batch_size: %d,'
          'initiation timestamp: %s', batch_number, operation.value,
          task.start_index, task.batch_size, task.timestamp)
      return 'No items to process', http.HTTPStatus.OK

    method = OPERATION_TO_METHOD.get(operation)

    # Creates batch from items loaded from BigQuery
    original_batch, skipped_item_ids, batch_id_to_item_id = batch_creator.create_batch(
        batch_number, items, method)

    # Optimizes batch via Shoptimizer for upsert/prevent_expiring operations
    if operation != constants.Operation.DELETE and constants.SHOPTIMIZER_API_INTEGRATION_ON:
      batch_to_send_to_content_api = _create_optimized_batch(
          original_batch, batch_number, operation)
    else:
      batch_to_send_to_content_api = original_batch

    # Sends batch of items to Content API for Shopping
    api_client = content_api_client.ContentApiClient()
    successful_item_ids, item_failures = api_client.process_items(
        batch_to_send_to_content_api, batch_number, batch_id_to_item_id, method)

    result = process_result.ProcessResult(
        successfully_processed_item_ids=successful_item_ids,
        content_api_failures=item_failures,
        skipped_item_ids=skipped_item_ids)
  except errors.HttpError as http_error:
    error_status_code = http_error.resp.status
    error_reason = http_error.resp.reason
    result = _handle_content_api_error(error_status_code, error_reason,
                                       batch_number, http_error, items,
                                       operation, task)
    return error_reason, error_status_code
  except socket.timeout as timeout_error:
    error_status_code = http.HTTPStatus.REQUEST_TIMEOUT
    error_reason = 'Socket timeout'
    result = _handle_content_api_error(error_status_code, error_reason,
                                       batch_number, timeout_error, items,
                                       operation, task)
    return error_reason, error_status_code
  else:
    logging.info(
        'Batch #%d with operation %s and initiation timestamp %s successfully processed %s items, failed to process %s items and skipped %s items.',
        batch_number, operation.value, task.timestamp,
        result.get_success_count(), result.get_failure_count(),
        result.get_skipped_count())
  finally:
    recorder = result_recorder.ResultRecorder.from_service_account_json(
        constants.GCP_SERVICE_ACCOUNT_PATH, constants.DATASET_ID_FOR_MONITORING,
        constants.TABLE_ID_FOR_RESULT_COUNTS_MONITORING,
        constants.TABLE_ID_FOR_ITEM_RESULTS_MONITORING)
    recorder.insert_result(operation.value, result, task.timestamp,
                           batch_number)
  return 'OK', http.HTTPStatus.OK


def _load_items_from_bigquery(
    operation: constants.Operation,
    task: upload_task.UploadTask) -> List[bigquery.Row]:
  """Loads items from BigQuery.

  Args:
    operation: The operation to be performed on this batch of items.
    task: The Cloud Task object that initiated this request.

  Returns:
    The list of items loaded from BigQuery.
  """
  table_id = f'process_items_to_{operation.value}_{task.timestamp}'
  bq_client = bigquery_client.BigQueryClient.from_service_account_json(
      constants.GCP_SERVICE_ACCOUNT_PATH, constants.DATASET_ID_FOR_PROCESSING,
      table_id)
  try:
    items_iterator = bq_client.load_items(task.start_index, task.batch_size)
  except errors.HttpError as http_error:
    logging.exception(
        'Error loading items from %s.%s. HTTP status: %s. Error: %s',
        constants.DATASET_ID_FOR_PROCESSING, table_id, http_error.resp.status,
        http_error.resp.reason)
    raise
  return list(items_iterator)


def _create_optimized_batch(batch: constants.Batch, batch_number: int,
                            operation: constants.Operation) -> constants.Batch:
  """Creates an optimized batch by calling the Shoptimizer API.

  Args:
    batch: The batch of product data to be optimized.
    batch_number: The number that identifies this batch.
    operation: The operation to be performed on this batch (upsert, delete,
      prevent_expiring).

  Returns:
    The batch returned from the Shoptimizer API Client.
  """
  try:
    optimization_client = shoptimizer_client.ShoptimizerClient(
        batch_number, operation)
  except (OSError, ValueError):
    return batch

  return optimization_client.shoptimize(batch)


def _handle_content_api_error(
    error_status_code: int, error_reason: str, batch_num: int, error: Exception,
    item_rows: List[bigquery.Row], operation: constants.Operation,
    task: upload_task.UploadTask) -> process_result.ProcessResult:
  """Logs network related errors returned from Content API and returns a list of item failures.

  Args:
    error_status_code: HTTP status code from Content API.
    error_reason: The reason for the error.
    batch_num: The batch number.
    error: The error thrown by Content API.
    item_rows: The items being processed in this batch.
    operation: The operation to be performed on this batch of items.
    task: The Cloud Task object that initiated this request.

  Returns:
    The list of items that failed due to the error, wrapped in a
    process_result.
  """
  logging.warning(
      'Batch #%d with operation %s and initiation timestamp %s failed. HTTP status: %s. Error: %s',
      batch_num, operation.value, task.timestamp, error_status_code,
      error_reason)
  # If the batch API call received an HttpError, mark every id as failed.
  item_failures = [
      failure.Failure(str(item_row.get('item_id', 'Missing ID')), error_reason)
      for item_row in item_rows
  ]
  api_result = process_result.ProcessResult([], item_failures, [])

  if content_api_client.suggest_retry(
      error_status_code) and _get_execution_attempt() < TASK_RETRY_LIMIT:
    logging.warning(
        'Batch #%d with operation %s and initiation timestamp %s will be requeued for retry',
        batch_num, operation.value, task.timestamp)
  else:
    logging.error(
        'Batch #%d with operation %s and initiation timestamp %s failed and will not be retried. Error: %s',
        batch_num, operation.value, task.timestamp, error)

  return api_result


def _get_execution_attempt() -> int:
  """Returns the number of times this task has previously been executed.

  If the execution count header does not exist, it means the request did not
  come from Cloud Tasks.
  In this case, there will be no retry, so set execution attempt to the retry
  limit.

  Returns:
    int, the number of times this task has previously been executed.
  """
  execution_attempt = flask.request.headers.get(
      'X-AppEngine-TaskExecutionCount', '')
  if execution_attempt:
    return int(execution_attempt)
  else:
    return TASK_RETRY_LIMIT


if __name__ == '__main__':
  # This is used when running locally. Gunicorn is used to run the
  # application on Google App Engine. See entrypoint in app.yaml.
  app.run(host='127.0.0.1', port=8080, debug=True)
