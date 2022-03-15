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

"""Record the number of items for each request to BigQuery."""

from typing import Any, Dict, List, Sequence
import logging

from google.cloud import bigquery
from google.cloud import exceptions as cloud_exceptions
from google.api_core import exceptions as api_exceptions

import constants
from models import failure
from models import process_result

# Count results table columns.
_COUNT_RESULTS_TABLE_COLUMN_CHANNEL = 'channel'
_COUNT_RESULTS_TABLE_COLUMN_OPERATION = 'operation'
_COUNT_RESULTS_TABLE_COLUMN_TIMESTAMP = 'timestamp'
_COUNT_RESULTS_TABLE_COLUMN_BATCH_ID = 'batch_id'
_COUNT_RESULTS_TABLE_COLUMN_SUCCESS_COUNT = 'success_count'
_COUNT_RESULTS_TABLE_COLUMN_FAILURE_COUNT = 'failure_count'
_COUNT_RESULTS_TABLE_COLUMN_SKIPPED_COUNT = 'skipped_count'

# Item ID results table columns.
_ITEM_RESULTS_TABLE_COLUMN_ITEM_ID = 'item_id'
_ITEM_RESULTS_TABLE_COLUMN_BATCH_ID = 'batch_id'
_ITEM_RESULTS_TABLE_COLUMN_CHANNEL = 'channel'
_ITEM_RESULTS_TABLE_COLUMN_OPERATION = 'operation'
_ITEM_RESULTS_TABLE_COLUMN_RESULT = 'result'
_ITEM_RESULTS_TABLE_COLUMN_ERROR = 'error'
_ITEM_RESULTS_TABLE_COLUMN_TIMESTAMP = 'timestamp'


class ResultRecorder(object):
  """BigQuery Client to record a Content API response to a table."""

  def __init__(self, client: bigquery.Client,
               count_results_table_reference: bigquery.TableReference,
               item_results_table_reference: bigquery.TableReference) -> None:
    """Initializes ContentApiResultRecorder.

    Args:
      client: bigquery.Client object.
      count_results_table_reference: Reference to the table containing the
        number of success/failure/skipped calls.
      item_results_table_reference: Reference to the table containing the
        Content API result for each id.
    """
    self._client = client
    self._count_results_table_reference = count_results_table_reference
    self._item_results_table_reference = item_results_table_reference

  @classmethod
  def from_service_account_json(cls, service_account_path: str, dataset_id: str,
                                count_results_table_id: str,
                                item_results_table_id: str) -> 'ResultRecorder':
    """Factory method to retrieve JSON credentials while creating client.

    Args:
      service_account_path: Path to service account configuration file.
      dataset_id: BigQuery dataset id to manipulate.
      count_results_table_id: Table id for the table containing the number of
        success/failure/skipped calls.
      item_results_table_id: Table id for the table containing the Content API
        result for each id.

    Returns:
      The client created with the retrieved JSON credentials.
    """
    client = bigquery.Client.from_service_account_json(service_account_path)
    dataset_reference = client.dataset(dataset_id)
    count_results_table_reference = dataset_reference.table(
        count_results_table_id)
    item_results_table_reference = dataset_reference.table(
        item_results_table_id)
    return cls(
        client=client,
        count_results_table_reference=count_results_table_reference,
        item_results_table_reference=item_results_table_reference)

  def insert_result(self, channel: constants.Channel,
                    operation: constants.Operation,
                    result: process_result.ProcessResult, timestamp: str,
                    batch_id: int) -> None:
    """Inserts the Content API success/failure/skipped counts and result for each item into Big Query.

    Args:
      channel: Shopping channel targeted on this batch.
      operation: Operation performed on this batch.
      result: Result of Content API call.
      timestamp: Timestamp used to identify a job.
      batch_id: Identifier for the batch.
    """
    self._insert_count_result(channel, operation, result, batch_id, timestamp)
    self._insert_item_result(channel, operation, result, batch_id, timestamp)

  def _insert_count_result(self, channel: constants.Channel,
                           operation: constants.Operation,
                           result: process_result.ProcessResult, batch_id: int,
                           timestamp: str) -> None:
    """Inserts the success/failure/skipped counts of items processed in a batch into BigQuery.

    Args:
      channel: Shopping channel targeted on this batch.
      operation: Operation performed on this batch.
      result: Result of Content API call.
      batch_id: Identifier for the batch.
      timestamp: Timestamp used to identify a job.
    """
    table = self._client.get_table(self._count_results_table_reference)
    data = [{
        _COUNT_RESULTS_TABLE_COLUMN_CHANNEL: channel.value,
        _COUNT_RESULTS_TABLE_COLUMN_OPERATION: operation.value,
        _COUNT_RESULTS_TABLE_COLUMN_TIMESTAMP: timestamp,
        _COUNT_RESULTS_TABLE_COLUMN_BATCH_ID: batch_id,
        _COUNT_RESULTS_TABLE_COLUMN_SUCCESS_COUNT: result.get_success_count(),
        _COUNT_RESULTS_TABLE_COLUMN_FAILURE_COUNT: result.get_failure_count(),
        _COUNT_RESULTS_TABLE_COLUMN_SKIPPED_COUNT: result.get_skipped_count()
    }]
    response = self._client.insert_rows(table, data)
    if not response:
      logging.info(
          'Channel %s operation %s timestamp %s batch #%d: The result of the Content API for Shopping call was successfully recorded to BigQuery. Success inserting into table %s.',
          channel.value, operation.value, timestamp, batch_id, table.table_id)
    else:
      logging.error(
          'Channel %s operation %s timestamp %s batch #%d: The result of the Content API for Shopping call was not recorded to BigQuery. Failure inserting into table %s. Results: %s.',
          channel.value, operation.value, timestamp, batch_id, table.table_id,
          result.get_counts_str())

  def _insert_item_result(self, channel: constants.Channel,
                          operation: constants.Operation,
                          result: process_result.ProcessResult,
                          batch_number: int, timestamp: str) -> None:
    """Inserts the results of Content API calls for each ID in the batch.

    Args:
      channel: Shopping channel targeted on this batch.
      operation: Operation performed in this batch.
      result: Result of Content API call.
      batch_number: Identifier for the batch.
      timestamp: Timestamp used to identify a job.
    """
    table = self._client.get_table(self._item_results_table_reference)

    # Convert item results to Big Query rows
    success_rows = _get_items_as_rows(result.successfully_processed_item_ids,
                                      batch_number, 'success', channel,
                                      operation, timestamp)
    failure_rows = _get_items_as_rows(result.content_api_failures, batch_number,
                                      'failure', channel, operation, timestamp)
    skipped_rows = _get_items_as_rows(result.skipped_item_ids, batch_number,
                                      'skipped', channel, operation, timestamp)
    # Concatenate Big Query rows
    data = []
    data.extend(success_rows)
    data.extend(failure_rows)
    data.extend(skipped_rows)

    # Insert rows into Big Query
    try:
      response = self._client.insert_rows(table, data)
      if not response:
        logging.info(
            'Channel %s operation %s timestamp %s batch #%d: The per item results of the Content API for Shopping call were successfully recorded to BigQuery. Success inserting into table %s.',
            channel.value, operation.value, timestamp, batch_number,
            table.table_id)
      else:
        logging.exception(
            'Channel %s operation %s timestamp %s batch #%d: The per item results of the Content API for Shopping call were not recorded to BigQuery. Failure inserting into table %s. Results: %s.',
            channel.value, operation.value, timestamp, batch_number,
            table.table_id, result.get_ids_str())
    except cloud_exceptions.GoogleCloudError as google_cloud_error:
      logging.exception(
          'Channel %s operation %s timestamp %s batch #%d: The per item results of the Content API for Shopping call were not recorded to BigQuery. Received a Google Cloud exception while trying to insert into table %s. Exception: %s. Results: %s.',
          channel.value, operation.value, timestamp, batch_number,
          table.table_id, google_cloud_error, result.get_ids_str())
    except api_exceptions.GoogleAPIError as google_api_error:
      logging.exception(
          'Channel %s operation %s timestamp %s batch #%d: The per item results of the Content API for Shopping call were not recorded to BigQuery. Received an API exception from while trying to insert into table %s. Exception: %s. Results: %s.',
          channel.value, operation.value, timestamp, batch_number,
          table.table_id, google_api_error, result.get_ids_str())


def _get_items_as_rows(items_to_add: Sequence[Any], batch_number: int,
                       result: str, channel: constants.Channel,
                       operation: constants.Operation,
                       timestamp: str) -> List[Dict[str, Any]]:
  """Adds Content API results to the data object to be inserted into Big Query.

  Args:
    items_to_add: The items to add.
    batch_number: The id of this batch.
    result: The result of the Content API call.
    channel: The shopping channel targeted for these items.
    operation: The operation performed on these items.
    timestamp: The timestamp associated with this batch.

  Returns:
    A list of rows to insert into Big Query.
  """
  rows = []
  for item in items_to_add:
    rows.append({
        _ITEM_RESULTS_TABLE_COLUMN_ITEM_ID:
            item.item_id if isinstance(item, failure.Failure) else item,
        _ITEM_RESULTS_TABLE_COLUMN_BATCH_ID:
            batch_number,
        _ITEM_RESULTS_TABLE_COLUMN_CHANNEL:
            channel.value,
        _ITEM_RESULTS_TABLE_COLUMN_OPERATION:
            operation.value,
        _ITEM_RESULTS_TABLE_COLUMN_RESULT:
            result,
        _ITEM_RESULTS_TABLE_COLUMN_ERROR:
            item.error_msg if isinstance(item, failure.Failure) else '',
        _ITEM_RESULTS_TABLE_COLUMN_TIMESTAMP:
            timestamp,
    })
  return rows
