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
"""Unit tests for bq_to_pubsub_operator."""

import datetime
import unittest
import unittest.mock as mock

import airflow
from airflow import models
from airflow.contrib.hooks import gcp_pubsub_hook
import pandas as pd

from sfo_plugin import bq_to_pubsub_operator

DAG_ID = 'test_dag'
TASK_ID = 'test_task'
PROJECT_ID = 'test-project'
DATASET_ID = 'test_dataset'
TABLE_ID = 'test_table'
QUERY_FILE_PATH = 'queries/dummy_query.sql'
TOPIC_NAME = 'test-topic'

DUMMY_INSERT_SUCCESS = 1
DUMMY_INSERT_FAILURE = 2
DUMMY_INSERT_SKIPPED = 3
DUMMY_DELETE_SUCCESS = 4
DUMMY_DELETE_FAILURE = 5
DUMMY_DELETE_SKIPPED = 6
DUMMY_PREVENT_EXPIRING_SUCCESS = 7
DUMMY_PREVENT_EXPIRING_FAILURE = 8
DUMMY_PREVENT_EXPIRING_SKIPPED = 9

OPERATION_UPSERT = 'upsert'
OPERATION_DELETE = 'delete'
OPERATION_PREVENT_EXPIRING = 'prevent_expiring'

KEY_OPERATION = 'operation'
KEY_SUCCESS_COUNT = 'success_count'
KEY_FAILURE_COUNT = 'failure_count'
KEY_SKIPPED_COUNT = 'skipped_count'

BQ_COLUMNS = [
    KEY_OPERATION, KEY_SUCCESS_COUNT, KEY_FAILURE_COUNT, KEY_SKIPPED_COUNT
]

DUMMY_QUERY_RESULTS = ([
    OPERATION_UPSERT, DUMMY_INSERT_SUCCESS, DUMMY_INSERT_FAILURE,
    DUMMY_INSERT_SKIPPED
], [
    OPERATION_DELETE, DUMMY_DELETE_SUCCESS, DUMMY_DELETE_FAILURE,
    DUMMY_DELETE_SKIPPED
], [
    OPERATION_PREVENT_EXPIRING, DUMMY_PREVENT_EXPIRING_SUCCESS,
    DUMMY_PREVENT_EXPIRING_FAILURE, DUMMY_PREVENT_EXPIRING_SKIPPED
])


class GetRunResultsAndTriggerReportingTest(unittest.TestCase):

  def setUp(self):
    super(GetRunResultsAndTriggerReportingTest, self).setUp()
    self._dag = models.DAG(dag_id=DAG_ID, start_date=datetime.datetime.now())
    self._task = bq_to_pubsub_operator.GetRunResultsAndTriggerReportingOperator(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        query_file_path=QUERY_FILE_PATH,
        topic_name=TOPIC_NAME,
        dag=self._dag,
        task_id=TASK_ID)
    self._context = mock.MagicMock()
    self._mock_bq_hook = mock.patch(
        'airflow.contrib.hooks.bigquery_hook.BigQueryHook',
        autospec=True).start()
    self._mock_pubsub_hook = mock.patch(
        'airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook',
        autospec=True).start()
    self.addCleanup(mock.patch.stopall)

  def test_execute_should_publish_message_to_pubsub(self):
    expected_content_api_results = (
        f'{{"{OPERATION_UPSERT}": {{"{KEY_OPERATION}": "{OPERATION_UPSERT}", '
        f'"{KEY_SUCCESS_COUNT}": {DUMMY_INSERT_SUCCESS}, '
        f'"{KEY_FAILURE_COUNT}": {DUMMY_INSERT_FAILURE}, '
        f'"{KEY_SKIPPED_COUNT}": {DUMMY_INSERT_SKIPPED}}}, '
        f'"{OPERATION_DELETE}": {{"{KEY_OPERATION}": "{OPERATION_DELETE}", '
        f'"{KEY_SUCCESS_COUNT}": {DUMMY_DELETE_SUCCESS}, '
        f'"{KEY_FAILURE_COUNT}": {DUMMY_DELETE_FAILURE}, '
        f'"{KEY_SKIPPED_COUNT}": {DUMMY_DELETE_SKIPPED}}}, '
        f'"{OPERATION_PREVENT_EXPIRING}": {{"{KEY_OPERATION}": '
        f'"{OPERATION_PREVENT_EXPIRING}", "{KEY_SUCCESS_COUNT}": '
        f'{DUMMY_PREVENT_EXPIRING_SUCCESS}, "{KEY_FAILURE_COUNT}": '
        f'{DUMMY_PREVENT_EXPIRING_FAILURE}, "{KEY_SKIPPED_COUNT}": '
        f'{DUMMY_PREVENT_EXPIRING_SKIPPED}}}}}')
    expected_publish_messages = [{
        'attributes': {
            'content_api_results': expected_content_api_results,
        }
    }]
    bq_result = pd.DataFrame(data=DUMMY_QUERY_RESULTS, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    self._task.execute(self._context)
    self._mock_pubsub_hook.return_value.publish.assert_called_with(
        PROJECT_ID, TOPIC_NAME, messages=expected_publish_messages)

  def test_execute_with_non_existing_query_file_path(self):
    incorrect_path = 'invalid_directory/dummy_query.sql'
    task = bq_to_pubsub_operator.GetRunResultsAndTriggerReportingOperator(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        query_file_path=incorrect_path,
        topic_name=TOPIC_NAME,
        dag=self._dag,
        task_id=TASK_ID)
    with self.assertRaises(airflow.AirflowException):
      task.execute(self._context)
    self._mock_pubsub_hook.return_value.publish.assert_not_called()

  def test_execute_with_pubsub_error(self):
    bq_result = pd.DataFrame(data=DUMMY_QUERY_RESULTS, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    self._mock_pubsub_hook.return_value.publish.side_effect = gcp_pubsub_hook.PubSubException(
    )
    with self.assertRaises(airflow.AirflowException):
      self._task.execute(self._context)

  def test_load_result_from_bigquery_when_query_result_has_three_operations(
      self):
    bq_result = pd.DataFrame(data=DUMMY_QUERY_RESULTS, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)
    insert_result = bq_to_pubsub_operator.RunResult(OPERATION_UPSERT,
                                                    DUMMY_INSERT_SUCCESS,
                                                    DUMMY_INSERT_FAILURE,
                                                    DUMMY_INSERT_SKIPPED)
    delete_result = bq_to_pubsub_operator.RunResult(OPERATION_DELETE,
                                                    DUMMY_DELETE_SUCCESS,
                                                    DUMMY_DELETE_FAILURE,
                                                    DUMMY_DELETE_SKIPPED)
    prevent_expiring_result = bq_to_pubsub_operator.RunResult(
        OPERATION_PREVENT_EXPIRING, DUMMY_PREVENT_EXPIRING_SUCCESS,
        DUMMY_PREVENT_EXPIRING_FAILURE, DUMMY_PREVENT_EXPIRING_SKIPPED)
    self.assertDictEqual(
        {
            OPERATION_UPSERT: insert_result,
            OPERATION_DELETE: delete_result,
            OPERATION_PREVENT_EXPIRING: prevent_expiring_result
        }, results)

  def test_load_result_from_bigquery_when_query_result_has_one_operation(self):
    query_result_list = [DUMMY_QUERY_RESULTS[0]]
    bq_result = pd.DataFrame(data=query_result_list, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)
    insert_result = bq_to_pubsub_operator.RunResult(OPERATION_UPSERT,
                                                    DUMMY_INSERT_SUCCESS,
                                                    DUMMY_INSERT_FAILURE,
                                                    DUMMY_INSERT_SKIPPED)
    self.assertDictEqual({OPERATION_UPSERT: insert_result}, results)

  def test_load_result_from_bigquery_when_query_result_is_empty(self):
    query_result_list = []
    bq_result = pd.DataFrame(data=query_result_list, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)
    self.assertDictEqual(dict(), results)

  def test_load_result_with_non_existing_query_path(self):
    with self.assertRaises(bq_to_pubsub_operator.BigQueryAPICallError):
      with mock.patch('builtins.open', side_effect=IOError):
        self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)

  def test_send_summary_to_pubsub(self):
    results = {
        OPERATION_UPSERT:
            bq_to_pubsub_operator.RunResult(OPERATION_UPSERT,
                                            DUMMY_INSERT_SUCCESS,
                                            DUMMY_INSERT_FAILURE,
                                            DUMMY_INSERT_SKIPPED)
    }
    self._task._send_run_results_to_pubsub(results)
    self._mock_pubsub_hook.return_value.publish.assert_called()


if __name__ == '__main__':
  unittest.main()
