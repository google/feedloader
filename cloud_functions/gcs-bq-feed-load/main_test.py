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

"""Unit tests for Feed File Import Function main.py."""
import io
import os

import unittest.mock as mock

from absl.testing import parameterized
from google.api_core import exceptions
from google.cloud import bigquery

import iso8601
import main

_ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000

_TEST_VALID_SCHEMA_CONFIG_FILE = """
{
  "mapping": [
  {"csvHeader": "id", "bqColumn": "item_id", "columnType": "STRING"},
  {"csvHeader": "title", "bqColumn": "title", "columnType": "STRING"}
  ]
}
"""

_TEST_INVALID_SCHEMA_CONFIG_FILE = """
{
  "mapping": [
  {"csvHeader": "id", "bqColumn": "item_id", "columnType": "STRING"},
  {"csvHeader": "title", "bqColumn": "title"}
  ]
}
"""

_TEST_BQ_DATASET = 'feed-dataset'
_TEST_COMPLETED_BUCKET = 'completed-bucket'
_TEST_FILENAME = 'feedfile'
_TEST_UPDATE_BUCKET = 'update-bucket'


@mock.patch.dict(
    os.environ, {
        'BQ_DATASET': _TEST_BQ_DATASET,
        'COMPLETED_FILES_BUCKET': _TEST_COMPLETED_BUCKET,
        'UPDATE_BUCKET': _TEST_UPDATE_BUCKET,
    })
@mock.patch(
    'main._get_current_time_in_utc',
    return_value=iso8601.parse_date('2021-06-05T08:16:25.183Z'))
@mock.patch(
    'builtins.open',
    new_callable=mock.mock_open,
    read_data='{"mapping": [{"csvHeader": "id", "bqColumn": "item_id", "columnType": "STRING"}]}'
)
class ImportStorageFileIntoBigQueryTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.event = {
        'bucket': 'feed-bucket',
        'name': _TEST_FILENAME,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    self.context = mock.MagicMock()
    self.context.event_id = '12345'
    self.context.event_type = 'gcs-event'
    self.context.timestamp = '2021-06-05T08:16:15.183Z'

  def test_import_storage_file_drops_event_if_runtime_exceeds_max(
      self, _, mock_get_current_time_in_utc):
    long_running_datetime = '2021-06-05T09:16:15.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        long_running_datetime)

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn('Dropping event 12345', mock_logging.output[0])

  @mock.patch.dict(os.environ, {'UPDATE_BUCKET': ''})
  def test_import_storage_file_into_big_query_reports_error_on_missing_update_bucket_env_var(
      self, mock_open_file, mock_get_current_time_in_utc):
    del mock_open_file
    del mock_get_current_time_in_utc

    with self.assertLogs(level='ERROR') as mock_logging:
      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn('Update Bucket Environment Variable not found.',
                    mock_logging.output[0])

  def test_import_storage_file_into_big_query_reports_error_on_nonexistent_bucket(
      self, mock_open_file, mock_get_current_time_in_utc):
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_storage_client.return_value.get_bucket.side_effect = (
          exceptions.NotFound('404'))

      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn('Bucket update-bucket could not be found.',
                    mock_logging.output[0])

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_validates_valid_schema(
      self, mock_save_imported_filename, mock_perform_bq_load,
      mock_file_to_import_exists, mock_open_file, mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_perform_bq_load
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'builtins.open',
        new_callable=mock.mock_open,
        read_data=_TEST_VALID_SCHEMA_CONFIG_FILE):
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_storage_client.assert_called()

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_returns_on_invalid_schema(
      self, mock_save_imported_filename, mock_perform_bq_load,
      mock_file_to_import_exists, mock_open_file, mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_perform_bq_load
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging, mock.patch(
                'builtins.open',
                new_callable=mock.mock_open,
                read_data=_TEST_INVALID_SCHEMA_CONFIG_FILE):
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn('Schema is invalid', mock_logging.output[0])

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_logs_file_import(
      self, mock_save_imported_filename, mock_perform_bq_load,
      mock_file_to_import_exists, mock_open_file, mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_perform_bq_load
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn(f'Uploaded Filename: feed-bucket/{_TEST_FILENAME}',
                    mock_stdout.getvalue())

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_calls_get_bucket(
      self, mock_save_imported_filename, mock_perform_bq_load,
      mock_file_to_import_exists, mock_open_file, mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_perform_bq_load
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_get_bucket.return_value.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_get_bucket.assert_called_with(_TEST_UPDATE_BUCKET)

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_checks_for_eof(
      self, mock_save_imported_filename, mock_perform_bq_load,
      mock_file_to_import_exists, mock_open_file, mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_perform_bq_load
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_get_bucket.get_blob.assert_called_with('EOF')

  def test_import_storage_file_into_big_query_throws_error_if_eof_exists(
      self, mock_open_file, mock_get_current_time_in_utc):
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:

      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn('An EOF file was found in bucket', mock_logging.output[0])

  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_loads_data_to_bq_successfully(
      self, mock_save_imported_filename, mock_open_file,
      mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client') as mock_bigquery_client:

      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_bigquery_client.return_value.load_table_from_uri.assert_called_with(
          'gs://feed-bucket/feedfile',
          f'{_TEST_BQ_DATASET}.items',
          job_config=mock.ANY)

  def test_import_storage_file_into_big_query_saves_filename_to_completed_bucket_after_bq_load(
      self, mock_open_file, mock_get_current_time_in_utc):
    del mock_open_file
    del mock_get_current_time_in_utc

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client') as mock_bigquery_client, mock.patch(
            'sys.stdout', new_callable=io.StringIO) as mock_stdout:

      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_bigquery_client.return_value.load_table_from_uri.assert_called()
      self.assertIn(
          f'{_TEST_FILENAME} was saved into GCS bucket: '
          f'{_TEST_COMPLETED_BUCKET}',
          mock_stdout.getvalue())

  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_called_with_converted_schema_config(
      self, mock_save_imported_filename, mock_open_file,
      mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_open_file
    del mock_get_current_time_in_utc

    expected_schema = [
        bigquery.SchemaField('item_id', 'STRING'),
        bigquery.SchemaField('title', 'STRING')
    ]

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.LoadJobConfig'
    ) as mock_big_query_load_job_config, mock.patch(
        'main.bigquery.Client'), mock.patch(
            'builtins.open',
            new_callable=mock.mock_open,
            read_data=_TEST_VALID_SCHEMA_CONFIG_FILE):
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None

      main.import_storage_file_into_big_query(self.event, self.context)

      mock_big_query_load_job_config.assert_called_with(
          allow_jagged_rows=True,
          autodetect=True,
          encoding='UTF-8',
          field_delimiter='\t',
          quote_character='',
          schema=expected_schema,
          skip_leading_rows=1,
          source_format=bigquery.SourceFormat.CSV,
          time_partitioning=bigquery.table.TimePartitioning(
              type_='DAY', expiration_ms=_ITEMS_TABLE_EXPIRATION_DURATION_MS))

  @mock.patch('main._save_imported_filename_to_gcs')
  def test_import_storage_file_into_big_query_catches_bq_load_exception(
      self, mock_save_imported_filename, mock_open_file,
      mock_get_current_time_in_utc):
    del mock_save_imported_filename
    del mock_open_file
    del mock_get_current_time_in_utc

    test_job_id = 5678

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client') as mock_bigquery_client, self.assertLogs(
            level='ERROR') as mock_logging:

      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None

      mock_load_job = mock_bigquery_client.return_value.load_table_from_uri.return_value
      mock_load_job.job_id = test_job_id
      mock_load_job.result.side_effect = exceptions.BadRequest('Bad Request')

      main.import_storage_file_into_big_query(self.event, self.context)

      self.assertIn(f'BigQuery load job {test_job_id} failed to finish.',
                    mock_logging.output[0])
