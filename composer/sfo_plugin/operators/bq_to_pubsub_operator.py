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

"""Airflow operator to read process result from BigQuery and send it to Cloud Pub/Sub to trigger reporting."""

import json
import logging
import string
from typing import Any, Dict, List, Mapping

import airflow
from airflow import models
from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcp_pubsub_hook
import attr
import pandas_gbq

_KEY_CHANNEL = 'channel'
_KEY_OPERATION = 'operation'
_KEY_SUCCESS_COUNT = 'success_count'
_KEY_FAILURE_COUNT = 'failure_count'
_KEY_SKIPPED_COUNT = 'skipped_count'


@attr.s(auto_attribs=True)
class RunResult(object):
  """Class to save the result of Content API call."""
  channel: str
  operation: str
  success_count: int
  failure_count: int
  skipped_count: int


def _convert_run_result_into_json(result: RunResult) -> Dict[str, Any]:
  """Convert ContentApiResult to JSON-encodable dict.

  Args:
    result: A ContentApiResult object.

  Returns:
    JSON-encodable dict to be included in a Pub/Sub message.
  """
  logging.info(
      'Creating result dictionary for PubSub: %s, %s, %s, %s, %s',
      result.channel,
      result.operation,
      result.success_count,
      result.failure_count,
      result.skipped_count,
  )
  result_dict = {
      _KEY_CHANNEL: result.channel,
      _KEY_OPERATION: result.operation,
      _KEY_SUCCESS_COUNT: result.success_count,
      _KEY_FAILURE_COUNT: result.failure_count,
      _KEY_SKIPPED_COUNT: result.skipped_count,
  }
  return result_dict


def _generate_query_string(
    filepath: str, project_id: str, dataset_id: str, table_id: str
) -> str:
  """Generates string format of a query.

  Args:
    filepath: File path of a query file. The content of the file is BigQuery
      Standard SQL query. Lines starting with '#' are recognized as comments and
      omitted in the returned string. To get to know about BigQuery Standard
      SQL, read
        https://cloud.google.com/bigquery/docs/reference/standard-sql/
    project_id: GCP project ID.
    dataset_id: BigQuery dataset ID.
    table_id: BigQuery table ID.

  Returns:
    Google SQL query including GCP project id.

  Raises:
    GenerateQueryError: an error occurs when the query file does not exist.
  """
  try:
    with open(filepath) as query_file:
      query_string = ''.join(
          line for line in query_file.readlines() if not line.startswith('#'))
      query_template = string.Template(query_string)
      return query_template.substitute(
          project_id=project_id, dataset_id=dataset_id, table_id=table_id)
  except IOError as io_error:
    raise GenerateQueryError(filepath) from io_error


class GetRunResultsAndTriggerReportingOperator(models.BaseOperator):
  """Reads BigQuery results and sends it to Cloud Pub/Sub."""

  def __init__(self, project_id: str, dataset_id: str, table_id: str,
               query_file_path: str, topic_name: str, *args, **kwargs) -> None:
    """Initializes GetRunResultsAndTriggerReporting.

    Args:
      project_id: GCP project ID.
      dataset_id: BigQuery dataset ID.
      table_id: BigQuery table ID.
      query_file_path: File path of the query to get the last result.
      topic_name: Cloud Pub/Sub topic name.
      *args: Arguments to initialize the super class.
      **kwargs: Keyword arguments to initialize the super class.
    """
    super(GetRunResultsAndTriggerReportingOperator,
          self).__init__(*args, **kwargs)
    self._project_id = project_id
    self._dataset_id = dataset_id
    self._table_id = table_id
    self._query_file_path = query_file_path
    self._topic_name = topic_name

  def execute(self, context: Dict[str, Any]) -> None:
    """Executes operator.

    This method is invoked by Airflow to send process result read from BigQuery
    to Cloud Pub/Sub

    Args:
      context: Airflow context that contains references to related objects to
        the task instance.

    Raises:
      airflow.AirflowException: Raised when the task failed to call BigQuery
        API.
      CloudFunctionsContextError: Raised when the triggering bucket name could
        not be extracted.
    """
    trigger_bucket = context['dag_run'].conf['bucket']
    if not trigger_bucket:
      raise CloudFunctionsContextError('Bucket could not be found in context.')

    # Determine if this needs to trigger the Local Feed mail notification.
    local_inventory_feed_enabled = 'local' in trigger_bucket
    try:
      results = self._load_run_results_from_bigquery(
          self._query_file_path, local_inventory_feed_enabled
      )
      logging.info('Results queried from BigQuery: %s', results)
    except BigQueryAPICallError as bq_api_error:
      raise airflow.AirflowException(
          'Failed to call BigQuery API'
      ) from bq_api_error
    try:
      logging.info(
          'Composer was triggered by EOF upload to bucket: %s', trigger_bucket)
      self._send_run_results_to_pubsub(
          results, local_inventory_feed_enabled
      )
    except PubSubAPICallError as pubsub_api_error:
      raise airflow.AirflowException(
          'Failed to call Cloud Pub/Sub API'
      ) from pubsub_api_error

  def _load_run_results_from_bigquery(
      self, query_file_path: str, local_inventory_feed_enabled: bool
  ) -> List[RunResult]:
    """Reads process result from BigQuery and return it as a dictionary.

    Args:
      query_file_path: File path of the query to get the last result.
      local_inventory_feed_enabled: True if local feeds are processed,
        otherwise False.

    Returns:
      A dict including number of processed items loaded from BigQuery.

    Raises:
      BigQueryAPICallError: an error occurs when BigQuery API call failed.
    """
    try:
      dataset_id = (
          f'{self._dataset_id}_local'
          if local_inventory_feed_enabled
          else self._dataset_id
      )
      query = _generate_query_string(
          query_file_path, self._project_id, dataset_id, self._table_id
      )
    except GenerateQueryError as query_error:
      logging.error('Query file does not exist: %s', query_error.filepath)
      raise BigQueryAPICallError(
          'BigQuery API call will not happen because query file does not exist.'
      ) from query_error
    bq_hook = bigquery_hook.BigQueryHook(use_legacy_sql=False)
    try:
      query_result = bq_hook.get_pandas_df(sql=query)
    except pandas_gbq.gbq.GenericGBQException as bgq_error:
      raise BigQueryAPICallError(
          'BigQuery API call got an error') from bgq_error
    results = []
    for _, row in query_result.iterrows():
      result = RunResult(
          channel=row[_KEY_CHANNEL],
          operation=row[_KEY_OPERATION],
          success_count=row[_KEY_SUCCESS_COUNT],
          failure_count=row[_KEY_FAILURE_COUNT],
          skipped_count=row[_KEY_SKIPPED_COUNT],
      )
      results.append(result)
    return results

  def _send_run_results_to_pubsub(
      self, results: Mapping[str, RunResult], local_inventory_feed_enabled: bool
  ) -> None:
    """Sends process result to Cloud Pub/Sub.

    Args:
      results: Results of Content API calls in a run.
      local_inventory_feed_enabled: True if local feeds are processed,
        otherwise False.

    Raises:
      PubSubAPICallError: an error occurs when Cloud Pub/Sub API call failed.
    """
    message = {
        'attributes': {
            'content_api_results': json.dumps(
                results, default=_convert_run_result_into_json
            ),
            'local_inventory_feed_enabled': local_inventory_feed_enabled
        }
    }
    logging.info('Message constructed for mailer: %s', message)
    pubsub_hook = gcp_pubsub_hook.PubSubHook()

    try:
      pubsub_hook.publish(
          project_id=self._project_id,
          topic=self._topic_name,
          messages=[message],
      )

      logging.info(
          (
              'Cloud Pub/Sub message was successfully sent to'
              ' /projects/%s/topics/%s. The content of the message: %s'
          ),
          self._project_id,
          self._topic_name,
          message,
      )
    except gcp_pubsub_hook.PubSubException as pubsub_error:
      raise PubSubAPICallError(
          'Cloud Pub/Sub message was not sent to'
          f' /projects/{self._project_id}/topics/{self._topic_name}'
      ) from pubsub_error


class GenerateQueryError(Exception):
  """Raised when generating query failed.

  Attributes:
    filepath: string, file path to a query file.
  """

  def __init__(self, filepath) -> None:
    super(GenerateQueryError, self).__init__()
    self.filepath = filepath


class BigQueryAPICallError(Exception):
  """Raised when BigQuery API call failed.

  Attributes:
    message: string, explanation of the error.
  """

  def __init__(self, message) -> None:
    super(BigQueryAPICallError, self).__init__()
    self.message = message


class PubSubAPICallError(Exception):
  """Raised when Cloud Pub/Sub API call failed.

  Attributes:
    message: string, explanation of the error.
  """

  def __init__(self, message) -> None:
    super(PubSubAPICallError, self).__init__()
    self.message = message


class CloudFunctionsContextError(Exception):
  """Raised when Cloud Functions failed to pass the context to Cloud Composer.

  Attributes:
    message: string, explanation of the error.
  """

  def __init__(self, message) -> None:
    super(CloudFunctionsContextError, self).__init__()
    self.message = message
