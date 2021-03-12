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

"""Unit tests for BigQueryClient."""

import unittest
import unittest.mock

from google import cloud
from google.cloud.bigquery import dataset
from google.cloud.bigquery import table

import bigquery_client

DATASET_ID = 'test_dataset'
TABLE_ID = 'test_table'
PROJECT_ID = 'dummy-project'
DUMMY_QUERY_FILEPATH = 'queries_test/dummy_query.sql'


def _build_mock_client(non_existing_dataset_id='', non_existing_table_id=''):
  """Build mock bigquery.Client object.

  The mock client simulates a situation where all possible datasets and tables
  exist except the ones provided as arguments. It raises an exception when an
  argument of get_dataset and get_table is equal to arguments provided to this
  function.

  Args:
    non_existing_dataset_id: String, the id of dataset that is not existing.
    non_existing_table_id: String, the id of table that is not existing.

  Returns:
    Mock bigquery.Client object.
  """

  def raise_exception_for_dataset(dataset_reference):
    """Raise an exception when dataset_reference refers to the specific dataset.

    Args:
      dataset_reference: an object of bigquery.dataset.DatasetReference.

    Raises:
      cloud.exception.NotFound: an exception is raised when dataset_reference
      refers to given dataset id.
    """
    if dataset_reference.dataset_id == non_existing_dataset_id:
      raise cloud.exceptions.NotFound('')

  def raise_exception_for_table(table_reference):
    """Raise an exception when table_reference refers to the specific table.

    Args:
      table_reference: an object of bigquery.table.TableReference.

    Raises:
      cloud.exception.NotFound: an exception is raised when table_reference
      refers to given table id.
    """
    if table_reference.table_id == non_existing_table_id:
      raise cloud.exceptions.NotFound('')

  client = unittest.mock.Mock()
  client.get_dataset.side_effect = raise_exception_for_dataset
  client.get_table.side_effect = raise_exception_for_table
  client.project = PROJECT_ID
  return client


class BigQueryClientTest(unittest.TestCase):

  def setUp(self):
    super(BigQueryClientTest, self).setUp()
    self.dataset_reference = dataset.DatasetReference(PROJECT_ID, DATASET_ID)
    self.table_reference = table.TableReference(self.dataset_reference,
                                                TABLE_ID)
    self.dummy_query = bigquery_client.generate_query_string(
        DUMMY_QUERY_FILEPATH, PROJECT_ID)

  def test_initialize_dataset_and_table_when_neither_dataset_nor_table_exists(
      self):
    mock_client = _build_mock_client(
        non_existing_dataset_id=DATASET_ID, non_existing_table_id=TABLE_ID)
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.dataset_reference,
                                               self.table_reference)
    bq_client.initialize_dataset_and_table(self.dummy_query)
    mock_client.create_dataset.assert_called_with(self.dataset_reference)
    mock_client.query.assert_called()

  def test_initialize_dataset_and_table_when_dataset_not_exists(self):
    mock_client = _build_mock_client(non_existing_dataset_id=DATASET_ID)
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.dataset_reference,
                                               self.table_reference)
    bq_client.initialize_dataset_and_table(self.dummy_query)
    mock_client.create_dataset.assert_called_with(self.dataset_reference)
    mock_client.query.assert_not_called()

  def test_initialize_dataset_and_table_when_table_not_exists(self):
    mock_client = _build_mock_client(non_existing_table_id=TABLE_ID)
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.dataset_reference,
                                               self.table_reference)
    bq_client.initialize_dataset_and_table(self.dummy_query)
    mock_client.create_dataset.assert_not_called()
    mock_client.query.assert_called()

  def test_initialize_dataset_and_table_when_both_dataset_and_table_exist(self):
    mock_client = _build_mock_client()
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.dataset_reference,
                                               self.table_reference)
    bq_client.initialize_dataset_and_table(self.dummy_query)
    mock_client.create_dataset.assert_not_called()
    mock_client.query.assert_not_called()

  def test_delete_table(self):
    mock_client = _build_mock_client()
    bq_client = bigquery_client.BigQueryClient(mock_client,
                                               self.dataset_reference,
                                               self.table_reference)
    bq_client.delete_table()
    mock_client.delete_table.assert_called()


class ModuleFunctionsTest(unittest.TestCase):

  def test_generate_query_string(self):
    expected_generated_query = """/** A dummy query used in unittests. */
SELECT {};
""".format(PROJECT_ID)
    self.assertEqual(
        expected_generated_query,
        bigquery_client.generate_query_string(DUMMY_QUERY_FILEPATH, PROJECT_ID))

  def test_generate_query_string_with_non_existing_file(self):
    with self.assertRaises(IOError):
      bigquery_client.generate_query_string('wrong_filepath', PROJECT_ID)
