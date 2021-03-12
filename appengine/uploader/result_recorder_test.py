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

"""Unit tests for result_recorder.py."""

import unittest
from unittest import mock

from google.cloud import bigquery

from models import failure
from models import process_result
import result_recorder

_DATASET_ID = 'test_dataset'
_TABLE_ID_FOR_COUNTS = 'test_counts_table'
_TABLE_ID_FOR_RESULTS = 'test_api_results_table'
_PROJECT_ID = 'dummy-project'

_CORRECT_COUNTS_SCHEMA = {
    'operation': str,
    'timestamp': str,
    'batch_id': int,
    'success_count': int,
    'failure_count': int,
    'skipped_count': int
}

_CORRECT_ID_SCHEMA = {
    'item_id': str,
    'batch_id': int,
    'operation': str,
    'result': str,
    'error': str,
    'timestamp': str
}


def _build_mock_client():
  """Returns a mock BigQuery client."""

  def _mock_insert_rows(_, data):
    # Delete a parameter used in the real function but not in the mock function.
    response = []
    schema = _CORRECT_COUNTS_SCHEMA if _is_counts_data(
        data) else _CORRECT_ID_SCHEMA
    for row in data:
      for key, correct_type in schema.items():
        if not isinstance(row[key], correct_type):
          response.append({
              'index':
                  0,
              'errors': [{
                  'reason': 'invalid',
                  'location': '{}'.format(key),
                  'debugInfo': '',
                  'message': 'Cannot convert value to {}'.format(correct_type)
              }]
          })
    return response

  def _is_counts_data(data):
    return 'success_count' in data[0]

  client = mock.MagicMock()
  client.insert_rows.side_effect = _mock_insert_rows
  client.project = _PROJECT_ID
  return client


class ResultRecorderTest(unittest.TestCase):

  def setUp(self):
    super(ResultRecorderTest, self).setUp()
    self.client = _build_mock_client()
    dataset_reference = bigquery.DatasetReference(_PROJECT_ID, _DATASET_ID)
    self.count_results_table_reference = bigquery.TableReference(
        dataset_reference, _TABLE_ID_FOR_COUNTS)
    self.id_results_table_reference = bigquery.TableReference(
        dataset_reference, _TABLE_ID_FOR_RESULTS)
    self.recorder = result_recorder.ResultRecorder(
        client=self.client,
        count_results_table_reference=self.count_results_table_reference,
        item_results_table_reference=self.id_results_table_reference)

  def test_insert_count_result_with_correct_types(self):
    operation = 'insert'
    timestamp = '000101010100'
    batch_id = 0
    result = process_result.ProcessResult(
        ['0001'], [failure.Failure('0002', 'Error message')], ['0003'])

    self.recorder.insert_result(operation, result, timestamp, batch_id)

    self.client.insert_rows.assert_called()
