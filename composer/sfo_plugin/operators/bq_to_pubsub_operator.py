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

"""Airflow operator to read process result from BigQuery and send it to Cloud Pub/Sub to trigger reporting."""

import json
import logging
import string
from typing import Any, Dict, Mapping

import airflow
from airflow import models
from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcp_pubsub_hook
import attr
import pandas_gbq

_KEY_OPERATION = 'operation'
_KEY_SUCCESS_COUNT = 'success_count'
_KEY_FAILURE_COUNT = 'failure_count'
_KEY_SKIPPED_COUNT = 'skipped_count'


@attr.s(auto_attribs=True)
class RunResult(object):
  """Class to save the result of Content API call."""
  operation: str
  success_count: int
  failure_count: int
  skipped_count: int


def _convert_run_result_into_json(result: RunResult) -> Mapping[str, Any]:
  """Convert ContentApiResult to JSON-encodable dict.

  Args:
    result: A ContentApiResult object.

  Returns:
    JSON-encodable dict to be included in a Pub/Sub message.
  """
  result_dict = {
      _KEY_OPERATION: result.operation,
      _KEY_SUCCESS_COUNT: result.success_count,
      _KEY_FAILURE_COUNT: result.failure_count,
      _KEY_SKIPPED_COUNT: result.skipped_count
  }
  return result_dict


def _generate_query_string(filepath: str, project_id: str, dataset_id: str,
                           table_id: str) -> str:
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
  """Airflow operator to read process result from BigQuery and send it to Cloud Pub/Sub."""

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
    """
    try:
      results = self._load_run_results_from_bigquery(self._query_file_path)
    except BigQueryAPICallError as bq_api_error:
      raise airflow.AirflowException(
          'Failed to call BigQuery API') from bq_api_error
    try:
      self._send_run_results_to_pubsub(results)
    except PubSubAPICallError as pubsub_api_error:
      raise airflow.AirflowException(
          'Failed to call Cloud Pub/Sub API') from pubsub_api_error

  def _load_run_results_from_bigquery(
      self, query_file_path: str) -> Dict[str, RunResult]:
    """Reads process result from BigQuery and return it as a dictionary.

    Args:
      query_file_path: File path of the query to get the last result.

    Returns:
      A dict including number of processed items loaded from BigQuery.

    Raises:
      BigQueryAPICallError: an error occurs when BigQuery API call failed.
    """
    try:
      query = _generate_query_string(query_file_path, self._project_id,
                                     self._dataset_id, self._table_id)
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
    results = {}
    for _, row in query_result.iterrows():
      result = RunResult(
          operation=row[_KEY_OPERATION],
          success_count=row[_KEY_SUCCESS_COUNT],
          failure_count=row[_KEY_FAILURE_COUNT],
          skipped_count=row[_KEY_SKIPPED_COUNT])
      results[row[_KEY_OPERATION]] = result
    return results

  def _send_run_results_to_pubsub(self, results: Mapping[str,
                                                         RunResult]) -> None:
    """Sends process result to Cloud Pub/Sub.

    Args:
      results: Results of Content API calls in a run.

    Raises:
      PubSubAPICallError: an error occurs when Cloud Pub/Sub API call failed.
    """
    message = {
        'attributes': {
            'content_api_results':
                json.dumps(results, default=_convert_run_result_into_json)
        }
    }
    pubsub_hook = gcp_pubsub_hook.PubSubHook()
    try:
      pubsub_hook.publish(
          self._project_id, self._topic_name, messages=[message])
      logging.info(
          'Cloud Pub/Sub message was successfully sent to /projects/%s/topics/%s. The content of the message: %s',
          self._project_id, self._topic_name, message)
    except gcp_pubsub_hook.PubSubException as pubsub_error:
      raise PubSubAPICallError(
          f'Cloud Pub/Sub message was not sent to /projects/{self._project_id}/topics/{self._topic_name}'
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
