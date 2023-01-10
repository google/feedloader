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

"""Unit tests for the Reprocess Feed File Cloud Function."""
import os
import unittest.mock as mock

from absl.testing import parameterized
from google.cloud import bigquery

import main

_TEST_BQ_DATASET = 'feed-dataset'
_TEST_COMPLETED_BUCKET = 'completed-bucket'
_TEST_FEED_BUCKET = 'feed-bucket'
_TEST_RETRIGGER_BUCKET = 'retrigger-bucket'
_TEST_RETRIGGER_FILENAME = 'REPROCESS_TRIGGER_FILE'
_TEST_UPDATE_BUCKET = 'update-bucket'


@mock.patch.dict(
    os.environ, {
        'BQ_DATASET': _TEST_BQ_DATASET,
        'COMPLETED_FILES_BUCKET': _TEST_COMPLETED_BUCKET,
        'FEED_BUCKET': _TEST_FEED_BUCKET,
        'RETRIGGER_BUCKET': _TEST_RETRIGGER_BUCKET,
        'UPDATE_BUCKET': _TEST_UPDATE_BUCKET,
    })
class ReprocessFeedFileIntegrationTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.event = {
        'bucket': 'feed-bucket',
        'name': _TEST_RETRIGGER_FILENAME,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0',
    }
    self.context = mock.MagicMock()
    self.context.event_id = '12345'
    self.context.event_type = 'gcs-event'
    self.context.timestamp = '2021-06-05T08:16:15.183Z'

  @mock.patch('main._function_execution_exceeded_max_allowed_duration')
  @mock.patch('main._schema_config_valid')
  @mock.patch('main._parse_schema_config')
  @mock.patch('main._retrigger_calculation_function')
  def test_reprocess_feed_file_calls_retrigger_calculation_function_if_no_files_to_reprocess(
      self, mock_retrigger_calculation_function, mock_parse_schema_config,
      mock_schema_config_valid,
      mock_function_execution_exceeded_max_allowed_duration):
    test_config = {
        'mapping': [
            {
                'csvHeader': 'google_merchant_id',
                'bqColumn': 'google_merchant_id',
                'columnType': 'INTEGER',
            },
            {
                'csvHeader': 'title',
                'bqColumn': 'title',
                'columnType': 'STRING',
            },
        ],
    }
    test_bigquery_schema = [
        bigquery.SchemaField('google_merchant_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING'),
    ]

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client'), mock.patch(
            'builtins.open',
            mock.mock_open(read_data='')) as mock_file, mock.patch(
                'json.load') as mock_json_load:
      mock_json_load.return_value = test_config
      mock_function_execution_exceeded_max_allowed_duration.return_value = False
      mock_schema_config_valid.return_value = True
      mock_parse_schema_config.return_value = test_bigquery_schema
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_retrigger_bucket = mock_get_bucket.return_value
      mock_missing_files_blob = mock_retrigger_bucket.get_blob.return_value
      mock_missing_files_blob.download_as_string.return_value = ''

      main.reprocess_feed_file(self.event, self.context)

      mock_file.assert_called_with('config.json')
      mock_retrigger_calculation_function.assert_called()

  @mock.patch('main._function_execution_exceeded_max_allowed_duration')
  @mock.patch('main._schema_config_valid')
  @mock.patch('main._parse_schema_config')
  @mock.patch('main._reprocess_file')
  @mock.patch('main._reupload_file_list')
  def test_reprocess_feed_file_calls_reprocessing_helpers_if_missing_files_detected(
      self, mock_reupload_file_list, mock_reprocess_file,
      mock_parse_schema_config, mock_schema_config_valid,
      mock_function_execution_exceeded_max_allowed_duration):
    test_config = {
        'mapping': [
            {
                'csvHeader': 'google_merchant_id',
                'bqColumn': 'google_merchant_id',
                'columnType': 'INTEGER',
            },
            {
                'csvHeader': 'title',
                'bqColumn': 'title',
                'columnType': 'STRING',
            },
        ],
    }
    test_bigquery_schema = [
        bigquery.SchemaField('google_merchant_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING'),
    ]
    test_failed_filename_bytes = (
        'failed_feed_file_1.txt\nfailed_feed_file_2.txt'.encode())

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client'), mock.patch(
            'builtins.open', mock.mock_open(
                read_data='')), mock.patch('json.load') as mock_json_load:
      mock_json_load.return_value = test_config
      mock_function_execution_exceeded_max_allowed_duration.return_value = False
      mock_schema_config_valid.return_value = True
      mock_parse_schema_config.return_value = test_bigquery_schema
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_retrigger_bucket = mock_get_bucket.return_value
      mock_missing_files_blob = mock_retrigger_bucket.get_blob.return_value
      mock_missing_files_blob.download_as_string.return_value = (
          test_failed_filename_bytes)

      main.reprocess_feed_file(self.event, self.context)

      mock_reprocess_file.assert_called_with(mock.ANY, 'failed_feed_file_1.txt',
                                             test_bigquery_schema)
      mock_reupload_file_list.assert_called_with(mock.ANY,
                                                 ['failed_feed_file_2.txt'],
                                                 _TEST_RETRIGGER_FILENAME)

  @mock.patch('main._function_execution_exceeded_max_allowed_duration')
  @mock.patch('main._schema_config_valid')
  def test_reprocess_feed_file_logs_error_if_schema_config_was_invalid(
      self, mock_schema_config_valid,
      mock_function_execution_exceeded_max_allowed_duration):
    test_config = {
        'mapping': [
            {
                'csvHeader': 'google_merchant_id',
                'bqColumn': 'google_merchant_id',
                'columnType': 'INTEGER',
            },
            {
                'csvHeader': 'title',
                'bqColumn': 'title',
                'columnType': 'STRING',
            },
        ],
    }

    with mock.patch('builtins.open', mock.mock_open(read_data='')), mock.patch(
        'json.load') as mock_json_load, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_json_load.return_value = test_config
      mock_function_execution_exceeded_max_allowed_duration.return_value = False
      mock_schema_config_valid.return_value = False

      main.reprocess_feed_file(self.event, self.context)

      self.assertIn('Schema is invalid', mock_logging.output[0])
