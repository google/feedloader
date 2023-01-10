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

"""Unit tests for clean_up_operator."""

import datetime
import unittest
import unittest.mock as mock

from airflow import exceptions
from airflow import models
from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcs_hook
from google.cloud import exceptions as cloud_exceptions

from sfo_plugin.operators import clean_up_operator

_DAG_ID = 'test_dag'
_TASK_ID = 'test_task'
_DATASET_ID = 'test_dataset'
_TABLE_ID = 'test_table'
_BUCKET_ID = 'test_bucket'


class CleanUpOperatorTest(unittest.TestCase):

  def setUp(self) -> None:
    super(CleanUpOperatorTest, self).setUp()
    self._context = mock.MagicMock()
    self._mock_bq_hook = mock.patch.object(
        bigquery_hook, 'BigQueryHook', autospec=True).start()
    self._mock_gcs_hook = mock.patch.object(
        gcs_hook, 'GoogleCloudStorageHook', autospec=True).start()
    dag = models.DAG(dag_id=_DAG_ID, start_date=datetime.datetime.now())
    self._task = clean_up_operator.CleanUpOperator(
        dataset_id=_DATASET_ID,
        table_id=_TABLE_ID,
        bucket_id=_BUCKET_ID,
        dag=dag,
        task_id=_TABLE_ID)
    self.addCleanup(mock.patch.stopall)

  def test_execute_should_delete_items_table(self):
    self._task.execute(self._context)
    self._mock_bq_hook.return_value.get_cursor.return_value.run_table_delete.assert_called_with(
        deletion_dataset_table=f'{_DATASET_ID}.{_TABLE_ID}',
        ignore_if_missing=True)

  def test_execute_should_stop_airflow_with_bq_api_error(self):
    self._mock_bq_hook.return_value.get_cursor.return_value.run_table_delete.side_effect = Exception(
    )
    with self.assertRaises(exceptions.AirflowException):
      self._task.execute(self._context)

  def test_execute_should_delete_lock_file(self):
    self._task.execute(self._context)
    self._mock_gcs_hook.return_value.delete.assert_called_with(
        bucket=_BUCKET_ID, object='EOF.lock')

  def test_execute_should_stop_airflow_with_cloud_storage_error(self):
    self._mock_gcs_hook.return_value.delete.side_effect = cloud_exceptions.GoogleCloudError(
        message='GCP Error')
    with self.assertRaises(exceptions.AirflowException):
      self._task.execute(self._context)

  def test_execute_should_not_raise_airflow_error_when_lock_file_is_not_found(
      self):
    self._mock_gcs_hook.return_value.delete.side_effect = cloud_exceptions.NotFound(
        message='No such object')
    try:
      self._task.execute(self._context)
    except exceptions.AirflowException:
      self.fail('AirflowException was raised unexpectedly')

  def test_execute_should_delete_eof_lock(self):
    self._task.execute(self._context)
    self._mock_gcs_hook.return_value.delete.assert_called_with(
        bucket=_BUCKET_ID, object='EOF.lock')
