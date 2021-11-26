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

"""Unit tests for BigQueryClient."""

import unittest
import unittest.mock as mock

from google.cloud import bigquery

import bigquery_client

DATASET_ID = 'test_dataset'
TABLE_ID = 'test_table'
PROJECT_ID = 'dummy-project'
DUMMY_QUERY_FILEPATH = 'queries_test/dummy_query.sql'


class BigQueryClientTest(unittest.TestCase):

  def setUp(self):
    super(BigQueryClientTest, self).setUp()
    self.dataset_reference = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    self.table_reference = bigquery.TableReference(self.dataset_reference,
                                                   TABLE_ID)

  def test_load_items(self):
    mock_client = mock.MagicMock()
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.table_reference)
    start_index = 0
    batch_size = 1000
    tested_table = mock_client.get_table(self.table_reference)

    bq_client.load_items(start_index, batch_size)

    mock_client.list_rows.assert_called_with(
        tested_table, start_index=start_index, max_results=batch_size)
