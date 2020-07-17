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
"""Unit tests for custom bigquery_hook."""

import http
import unittest
import unittest.mock as mock

from googleapiclient import errors

from sfo_plugin import bigquery_hook

_PROJECT_ID = 'test_project_id'
_DATASET_ID = 'test_dataset'
_TABLE_ID = 'test_table'


class BigQueryCursorTest(unittest.TestCase):

  def setUp(self) -> None:
    super(BigQueryCursorTest, self).setUp()
    self._mock_service = mock.patch.object(
        bigquery_hook.BigQueryHook, 'get_service', autospec=False).start()
    self.bq_cursor = bigquery_hook.BigQueryHook()
    self.addCleanup(mock.patch.stopall)

  def test_delete_table_should_delete_given_table(self):
    try:
      self.bq_cursor.delete_table(dataset_id=_DATASET_ID, table_id=_TABLE_ID)
    # Using broad exception since this test should fail when any error happens.
    except Exception:  
      self.fail()
    # As projectId is set by Airflow environment, the value is not validated.
    self._mock_service.return_value.tables.return_value.delete.assert_called_with(
        projectId=mock.ANY, datasetId=_DATASET_ID, tableId=_TABLE_ID)

  def test_delete_table_should_succeed_when_table_does_not_exist(self):
    self._mock_service.return_value.tables.return_value.delete.return_value.execute.side_effect = errors.HttpError(
        resp=http.HTTPStatus.NOT_FOUND, content=b'Not Found')
    with self.assertLogs(level='INFO') as log:
      self.bq_cursor.delete_table(dataset_id=_DATASET_ID, table_id=_TABLE_ID)
      test_table_path = f'{_PROJECT_ID}.{_DATASET_ID}.{_TABLE_ID}'
      self.assertIn(f'INFO:root:Table does not exist: {test_table_path}',
                    log.output)

  def test_delete_table_should_raise_exception_with_http_error_other_than_not_found(
      self):
    self._mock_service.return_value.tables.return_value.delete.return_value.execute.side_effect = errors.HttpError(
        resp=http.HTTPStatus.INTERNAL_SERVER_ERROR, content=b'Backend Error')
    with self.assertRaises(bigquery_hook.BigQueryApiError):
      self.bq_cursor.delete_table(dataset_id=_DATASET_ID, table_id=_TABLE_ID)
