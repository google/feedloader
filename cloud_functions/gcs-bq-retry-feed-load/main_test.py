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

"""Unit tests for the Reprocess Feed File Cloud Function."""
import io
import os
import types
import unittest.mock as mock

from absl.testing import parameterized
from google.api_core import exceptions
from google.cloud import bigquery

import iso8601
import main

_ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000
_TEST_BQ_DATASET = 'feed-dataset'
_TEST_COMPLETED_BUCKET = 'completed-bucket'
_TEST_EOF_RETRY_FILENAME = main._EOF_RETRY_FILENAME
_TEST_FEED_BUCKET = 'feed-bucket'
_TEST_ITEMS_TABLE = main._ITEMS_TABLE_NAME
_TEST_INVALID_SCHEMA = {
    'invalid': [{
        'csvHeader': 'id',
        'bqColumn': 'item_id',
        'columnType': 'STRING',
    }, {
        'csvHeader': 'title',
        'bqColumn': 'title',
        'columnType': 'STRING',
    }]
}
_TEST_RETRIGGER_BUCKET = 'retrigger-bucket'
_TEST_RETRIGGER_FILENAME = 'REPROCESS_TRIGGER_FILE'
_TEST_UPDATE_BUCKET = 'update-bucket'
_TEST_VALID_SCHEMA_CONFIG_FILE = """
{
  "mapping": [
  {"csvHeader": "id", "bqColumn": "item_id", "columnType": "STRING"},
  {"csvHeader": "title", "bqColumn": "title", "columnType": "STRING"}
  ]
}
"""


@mock.patch.dict(
    os.environ, {
        'BQ_DATASET': _TEST_BQ_DATASET,
        'COMPLETED_FILES_BUCKET': _TEST_COMPLETED_BUCKET,
        'FEED_BUCKET': _TEST_FEED_BUCKET,
        'RETRIGGER_BUCKET': _TEST_RETRIGGER_BUCKET,
        'UPDATE_BUCKET': _TEST_UPDATE_BUCKET,
    })
@mock.patch(
    'main._get_current_time_in_utc',
    return_value=iso8601.parse_date('2021-06-05T08:16:25.183Z'))
class ReprocessFeedFileTest(parameterized.TestCase):

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

  def test_function_execution_exceeded_max_allowed_duration_returns_true_if_event_age_exceeded_max(
      self, mock_get_current_time_in_utc):
    long_running_datetime = '2021-06-05T09:16:15.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        long_running_datetime)

    result = main._function_execution_exceeded_max_allowed_duration(
        self.context)

    self.assertTrue(result)

  def test_function_execution_exceeded_max_allowed_duration_returns_false_if_event_age_was_within_max(
      self, mock_get_current_time_in_utc):
    long_running_datetime = '2021-06-05T08:20:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        long_running_datetime)

    result = main._function_execution_exceeded_max_allowed_duration(
        self.context)

    self.assertFalse(result)

  @mock.patch('main._perform_bigquery_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_reprocess_file_calls_big_query_load_and_saves_completed_filename(
      self, mock_save_imported_filename_to_gcs, mock_perform_bigquery_load, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
      test_failed_filename = 'failed_feed_file_1.txt'
      test_bigquery_schema = [
          bigquery.SchemaField('item_id', 'STRING'),
          bigquery.SchemaField('title', 'STRING'),
      ]

      main._reprocess_file(mock_storage_client, test_failed_filename,
                           test_bigquery_schema)

      mock_perform_bigquery_load.assert_called_with(_TEST_FEED_BUCKET,
                                                    test_failed_filename,
                                                    test_bigquery_schema)
      mock_save_imported_filename_to_gcs.assert_called_with(
          mock.ANY, test_failed_filename)

  def test_reupload_file_list_calls_upload_from_string_with_joined_filenames(
      self, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.get_bucket
      mock_blob = mock_get_bucket.return_value.blob
      mock_upload_from_string = mock_blob.return_value.upload_from_string
      test_failed_filenames = [
          'failed_feed_file_1.txt', 'failed_feed_file_2.txt',
      ]

      main._reupload_file_list(mock_storage_client, test_failed_filenames,
                               _TEST_RETRIGGER_FILENAME)

      mock_get_bucket.assert_called_with(_TEST_RETRIGGER_BUCKET)
      mock_blob.assert_called_with(_TEST_RETRIGGER_FILENAME)
      mock_upload_from_string.assert_called_with(
          'failed_feed_file_1.txt\nfailed_feed_file_2.txt')

  def test_retrigger_calculation_function_uploads_empty_file_to_gcs_bucket(
      self, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.get_bucket
      mock_blob = mock_get_bucket.return_value.blob
      mock_upload_from_string = mock_blob.return_value.upload_from_string

      main._retrigger_calculation_function(mock_storage_client)

      mock_get_bucket.assert_called_with(_TEST_UPDATE_BUCKET)
      mock_blob.assert_called_with(_TEST_EOF_RETRY_FILENAME)
      mock_upload_from_string.assert_called_with('')

  def test_schema_config_valid_returns_true_if_schema_was_valid(self, _):
    test_valid_bigquery_schema = {
        'mapping': [{
            'csvHeader': 'id',
            'bqColumn': 'item_id',
            'columnType': 'STRING',
        }, {
            'csvHeader': 'title',
            'bqColumn': 'title',
            'columnType': 'STRING',
        }]
    }

    result = main._schema_config_valid(test_valid_bigquery_schema)

    self.assertTrue(result)

  def test_schema_config_valid_returns_false_if_schema_was_invalid(self, _):
    test_invalid_schema = {
        'invalid': [{
            'csvHeader': 'id',
            'bqColumn': 'item_id',
            'columnType': 'STRING'
        }, {
            'csvHeader': 'title',
            'bqColumn': 'title',
            'columnType': 'STRING',
        }]
    }

    result = main._schema_config_valid(test_invalid_schema)

    self.assertFalse(result)

  def test_parse_schema_config_converts_schema_dict_into_bq_dict(self, _):
    test_valid_bigquery_schema = {
        'mapping': [{
            'csvHeader': 'id',
            'bqColumn': 'item_id',
            'columnType': 'STRING'
        }, {
            'csvHeader': 'title',
            'bqColumn': 'title',
            'columnType': 'STRING',
        }]
    }

    expected_schema = [
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING'),
    ]

    result = main._parse_schema_config(test_valid_bigquery_schema)

    self.assertEqual(expected_schema, result)

  def test_perform_bigquery_load_logs_error_on_failed_job_result(self, _):
    test_bigquery_schema = [
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING'),
    ]
    test_failed_filename = 'failed_feed_file_1.txt'
    with mock.patch(
        'main.bigquery.Client') as mock_bigquery_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_bigquery_load_job = mock_bigquery_client.return_value.load_table_from_uri
      mock_bigquery_load_job.return_value.result.side_effect = exceptions.GoogleAPICallError(
          'Bigquery Query Failed.')

      main._perform_bigquery_load(_TEST_FEED_BUCKET, test_failed_filename,
                                  test_bigquery_schema)

      self.assertIn('BigQuery load job failed.', mock_logging.output[0])

  def test_perform_bigquery_load_prints_loaded_rows(self, _):
    test_bigquery_schema = [
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING'),
    ]
    test_failed_filename = 'failed_feed_file_1.txt'
    with mock.patch('main.bigquery.Client') as mock_bigquery_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_load_table_from_uri = mock_bigquery_client.return_value.load_table_from_uri
      mock_bigquery_client.return_value.get_table.return_value = (
          types.SimpleNamespace(num_rows=100))

      main._perform_bigquery_load(_TEST_FEED_BUCKET, test_failed_filename,
                                  test_bigquery_schema)

      mock_load_table_from_uri.assert_called_with(
          f'gs://{_TEST_FEED_BUCKET}/{test_failed_filename}',
          f'{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}',
          job_config=mock.ANY)
      self.assertIn(
          f'Loaded 100 rows to table {_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}',
          mock_stdout.getvalue())

  def test_save_imported_filename_to_gcs_saves_file_and_logs_confirmation(
      self, _):
    test_failed_filename = 'failed_feed_file_1.txt'
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_get_bucket = mock_storage_client.get_bucket
      mock_completed_files_bucket = mock_get_bucket.return_value

      main._save_imported_filename_to_gcs(mock_storage_client,
                                          test_failed_filename)

      mock_get_bucket.assert_called_with(_TEST_COMPLETED_BUCKET)
      mock_completed_files_bucket.blob.assert_called_with(test_failed_filename)
      self.assertEqual(mock_get_bucket.call_args_list[0].args[0],
                       _TEST_COMPLETED_BUCKET)
      self.assertIn(
          f'Imported filename: {test_failed_filename} was saved into GCS bucket:',
          mock_stdout.getvalue())
