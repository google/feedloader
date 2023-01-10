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

"""Unit tests for wait_for_completion_operator.py."""

import datetime
import unittest
import unittest.mock as mock

import airflow
from airflow import models
from google.api_core import exceptions
from parameterized import parameterized

from sfo_plugin import wait_for_completion_operator

_DAG_ID = 'test_dag'
_TASK_ID = 'test_test'
_PROJECT_ID = 'test-project'
_QUEUE_LOCATION = 'test-location'
_QUEUE_NAME = 'test-queue'
_SERVICE_ACCOUNT_PATH = 'test_file_path'
_TRY_COUNT_LIMIT = 10


class WaitForCompletionOperatorTest(unittest.TestCase):

  def setUp(self):
    super(WaitForCompletionOperatorTest, self).setUp()
    dag = models.DAG(dag_id=_DAG_ID, start_date=datetime.datetime.now())
    self._task = wait_for_completion_operator.WaitForCompletionOperator(
        project_id=_PROJECT_ID,
        queue_location=_QUEUE_LOCATION,
        queue_name=_QUEUE_NAME,
        service_account_path=_SERVICE_ACCOUNT_PATH,
        try_count_limit=_TRY_COUNT_LIMIT,
        dag=dag,
        task_id=_TASK_ID)
    self._context = mock.MagicMock()
    self._mock_tasks_client = mock.patch(
        'google.cloud.tasks.CloudTasksClient.from_service_account_json',
        autospec=False).start()
    self._mock_time_sleep = mock.patch('time.sleep').start()
    self.addCleanup(mock.patch.stopall)

  @parameterized.expand([([['dummy_task_0', 'dummy_task_1', 'dummy_task_2'],
                           ['dummy_task_0'], []], 2), ([[]], 0)])
  def test_execute_should_wait_until_queue_becomes_empty(
      self, sequence_of_tasks, expected_sleep_count):
    self._mock_tasks_client.return_value.list_tasks.side_effect = sequence_of_tasks
    self._task.execute(self._context)
    self.assertEqual(expected_sleep_count, self._mock_time_sleep.call_count)

  @parameterized.expand([[exceptions.GoogleAPICallError('')],
                         [exceptions.RetryError('', Exception())],
                         [ValueError()]])
  def test_execute_when_exception_raised(self, api_exception):
    self._mock_tasks_client.return_value.list_tasks.side_effect = api_exception
    with self.assertRaises(airflow.AirflowException):
      self._task.execute(self._context)

  def test_execute_should_raise_exception_when_try_count_exceeds_limit(self):
    self._mock_tasks_client.return_value.list_tasks.return_value = [
        'blocked_task'
    ]
    with self.assertRaises(airflow.AirflowException):
      self._task.execute(self._context)
    self.assertEqual(_TRY_COUNT_LIMIT,
                     self._mock_tasks_client.return_value.list_tasks.call_count)
    self.assertEqual(_TRY_COUNT_LIMIT, self._mock_time_sleep.call_count)


if __name__ == '__main__':
  unittest.main()
