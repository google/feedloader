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

"""Unit tests for bq_to_pubsub_operator."""

import datetime
import json
import unittest
from unittest import mock

import airflow
from airflow import models
from airflow.contrib.hooks import gcp_pubsub_hook
import pandas as pd
from parameterized import parameterized

from sfo_plugin import bq_to_pubsub_operator

DAG_ID = 'test_dag'
TASK_ID = 'test_task'
PROJECT_ID = 'test-project'
DATASET_ID = 'test_dataset'
TABLE_ID = 'test_table'
QUERY_FILE_PATH = 'sfo_plugin/operators/queries/dummy_query.sql'
TOPIC_NAME = 'test-topic'
LOCAL_FEEDS_ENABLED = 'True'

DUMMY_INSERT_SUCCESS = 1
DUMMY_INSERT_FAILURE = 2
DUMMY_INSERT_SKIPPED = 3
DUMMY_DELETE_SUCCESS = 4
DUMMY_DELETE_FAILURE = 5
DUMMY_DELETE_SKIPPED = 6
DUMMY_PREVENT_EXPIRING_SUCCESS = 7
DUMMY_PREVENT_EXPIRING_FAILURE = 8
DUMMY_PREVENT_EXPIRING_SKIPPED = 9

CHANNEL_ONLINE = 'online'
CHANNEL_LOCAL = 'local'

OPERATION_UPSERT = 'upsert'
OPERATION_DELETE = 'delete'
OPERATION_PREVENT_EXPIRING = 'prevent_expiring'

KEY_CHANNEL = 'channel'
KEY_OPERATION = 'operation'
KEY_SUCCESS_COUNT = 'success_count'
KEY_FAILURE_COUNT = 'failure_count'
KEY_SKIPPED_COUNT = 'skipped_count'

BQ_COLUMNS = [
    KEY_CHANNEL,
    KEY_OPERATION,
    KEY_SUCCESS_COUNT,
    KEY_FAILURE_COUNT,
    KEY_SKIPPED_COUNT,
]

DUMMY_QUERY_RESULTS_FOR_ONLINE = (
    [
        CHANNEL_ONLINE,
        OPERATION_UPSERT,
        DUMMY_INSERT_SUCCESS,
        DUMMY_INSERT_FAILURE,
        DUMMY_INSERT_SKIPPED,
    ],
    [
        CHANNEL_ONLINE,
        OPERATION_DELETE,
        DUMMY_DELETE_SUCCESS,
        DUMMY_DELETE_FAILURE,
        DUMMY_DELETE_SKIPPED,
    ],
    [
        CHANNEL_ONLINE,
        OPERATION_PREVENT_EXPIRING,
        DUMMY_PREVENT_EXPIRING_SUCCESS,
        DUMMY_PREVENT_EXPIRING_FAILURE,
        DUMMY_PREVENT_EXPIRING_SKIPPED,
    ],
)
DUMMY_QUERY_RESULTS_FOR_LOCAL = (
    [
        CHANNEL_LOCAL,
        OPERATION_UPSERT,
        DUMMY_INSERT_SUCCESS,
        DUMMY_INSERT_FAILURE,
        DUMMY_INSERT_SKIPPED,
    ],
    [
        CHANNEL_LOCAL,
        OPERATION_DELETE,
        DUMMY_DELETE_SUCCESS,
        DUMMY_DELETE_FAILURE,
        DUMMY_DELETE_SKIPPED,
    ],
    [
        CHANNEL_LOCAL,
        OPERATION_PREVENT_EXPIRING,
        DUMMY_PREVENT_EXPIRING_SUCCESS,
        DUMMY_PREVENT_EXPIRING_FAILURE,
        DUMMY_PREVENT_EXPIRING_SKIPPED,
    ],
)
DUMMY_QUERY_RESULTS_FOR_BOTH_ONLINE_AND_LOCAL = (
    DUMMY_QUERY_RESULTS_FOR_ONLINE + DUMMY_QUERY_RESULTS_FOR_LOCAL
)


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

  @parameterized.expand([[3, DUMMY_QUERY_RESULTS_FOR_ONLINE],
                         [6, DUMMY_QUERY_RESULTS_FOR_BOTH_ONLINE_AND_LOCAL]])
  def test_execute_should_publish_message_to_pubsub(self,
                                                    expected_results_length,
                                                    dummy_query_results):
    expected_content_api_results_list = [
        {
            KEY_CHANNEL: CHANNEL_ONLINE,
            KEY_OPERATION: OPERATION_UPSERT,
            KEY_SUCCESS_COUNT: DUMMY_INSERT_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_INSERT_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_INSERT_SKIPPED
        },
        {
            KEY_CHANNEL: CHANNEL_ONLINE,
            KEY_OPERATION: OPERATION_DELETE,
            KEY_SUCCESS_COUNT: DUMMY_DELETE_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_DELETE_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_DELETE_SKIPPED
        },
        {
            KEY_CHANNEL: CHANNEL_ONLINE,
            KEY_OPERATION: OPERATION_PREVENT_EXPIRING,
            KEY_SUCCESS_COUNT: DUMMY_PREVENT_EXPIRING_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_PREVENT_EXPIRING_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_PREVENT_EXPIRING_SKIPPED
        },
        {
            KEY_CHANNEL: CHANNEL_LOCAL,
            KEY_OPERATION: OPERATION_UPSERT,
            KEY_SUCCESS_COUNT: DUMMY_INSERT_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_INSERT_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_INSERT_SKIPPED
        },
        {
            KEY_CHANNEL: CHANNEL_LOCAL,
            KEY_OPERATION: OPERATION_DELETE,
            KEY_SUCCESS_COUNT: DUMMY_DELETE_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_DELETE_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_DELETE_SKIPPED
        },
        {
            KEY_CHANNEL: CHANNEL_LOCAL,
            KEY_OPERATION: OPERATION_PREVENT_EXPIRING,
            KEY_SUCCESS_COUNT: DUMMY_PREVENT_EXPIRING_SUCCESS,
            KEY_FAILURE_COUNT: DUMMY_PREVENT_EXPIRING_FAILURE,
            KEY_SKIPPED_COUNT: DUMMY_PREVENT_EXPIRING_SKIPPED
        },
    ]
    expected_content_api_results = json.dumps(
        expected_content_api_results_list[:expected_results_length])
    expected_publish_messages = [{
        'attributes': {
            'content_api_results': expected_content_api_results,
        }
    }]
    bq_result = pd.DataFrame(data=dummy_query_results, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result

    self._task.execute(self._context)

    self._mock_pubsub_hook.return_value.publish.assert_called_with(
        project_id=PROJECT_ID,
        topic=TOPIC_NAME,
        messages=expected_publish_messages,
    )

  def test_execute_with_non_existing_query_file_path(self):
    incorrect_path = 'invalid_directory/dummy_query.sql'
    task = bq_to_pubsub_operator.GetRunResultsAndTriggerReportingOperator(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        query_file_path=incorrect_path,
        topic_name=TOPIC_NAME,
        dag=self._dag,
        task_id=TASK_ID + '_test_execute_with_non_existing_query_file_path',
    )

    with self.assertRaises(airflow.AirflowException):
      task.execute(self._context)

    self._mock_pubsub_hook.return_value.publish.assert_not_called()

  def test_execute_with_pubsub_error(self):
    bq_result = pd.DataFrame(
        data=DUMMY_QUERY_RESULTS_FOR_ONLINE, columns=BQ_COLUMNS
    )
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    self._mock_pubsub_hook.return_value.publish.side_effect = (
        gcp_pubsub_hook.PubSubException()
    )

    with self.assertRaises(airflow.AirflowException):
      self._task.execute(self._context)

  def test_load_result_from_bigquery_when_query_result_has_three_operations(
      self,
  ):
    bq_result = pd.DataFrame(
        data=DUMMY_QUERY_RESULTS_FOR_ONLINE, columns=BQ_COLUMNS
    )
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)

    insert_result = bq_to_pubsub_operator.RunResult(
        CHANNEL_ONLINE,
        OPERATION_UPSERT,
        DUMMY_INSERT_SUCCESS,
        DUMMY_INSERT_FAILURE,
        DUMMY_INSERT_SKIPPED,
    )
    delete_result = bq_to_pubsub_operator.RunResult(
        CHANNEL_ONLINE,
        OPERATION_DELETE,
        DUMMY_DELETE_SUCCESS,
        DUMMY_DELETE_FAILURE,
        DUMMY_DELETE_SKIPPED,
    )
    prevent_expiring_result = bq_to_pubsub_operator.RunResult(
        CHANNEL_ONLINE,
        OPERATION_PREVENT_EXPIRING,
        DUMMY_PREVENT_EXPIRING_SUCCESS,
        DUMMY_PREVENT_EXPIRING_FAILURE,
        DUMMY_PREVENT_EXPIRING_SKIPPED,
    )

    self.assertListEqual(
        [insert_result, delete_result, prevent_expiring_result], results
    )

  def test_load_result_from_bigquery_when_query_result_has_one_operation(self):
    query_result_list = [DUMMY_QUERY_RESULTS_FOR_ONLINE[0]]
    bq_result = pd.DataFrame(data=query_result_list, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result
    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)

    insert_result = bq_to_pubsub_operator.RunResult(
        CHANNEL_ONLINE,
        OPERATION_UPSERT,
        DUMMY_INSERT_SUCCESS,
        DUMMY_INSERT_FAILURE,
        DUMMY_INSERT_SKIPPED,
    )

    self.assertListEqual([insert_result], results)

  def test_load_result_from_bigquery_when_query_result_is_empty(self):
    query_result_list = []
    bq_result = pd.DataFrame(data=query_result_list, columns=BQ_COLUMNS)
    self._mock_bq_hook.return_value.get_pandas_df.return_value = bq_result

    results = self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)

    self.assertListEqual([], results)

  def test_load_result_with_non_existing_query_path(self):
    with self.assertRaises(bq_to_pubsub_operator.BigQueryAPICallError):
      with mock.patch('builtins.open', side_effect=IOError):
        self._task._load_run_results_from_bigquery(QUERY_FILE_PATH)

  def test_send_summary_to_pubsub(self):
    results = {
        OPERATION_UPSERT:
            bq_to_pubsub_operator.RunResult(CHANNEL_ONLINE, OPERATION_UPSERT,
                                            DUMMY_INSERT_SUCCESS,
                                            DUMMY_INSERT_FAILURE,
                                            DUMMY_INSERT_SKIPPED)
    }

    self._task._send_run_results_to_pubsub(results)

    self._mock_pubsub_hook.return_value.publish.assert_called()


if __name__ == '__main__':
  unittest.main()
