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

import unittest
import unittest.mock as mock

from google.cloud import exceptions

import iso8601
import main

_TEST_UPDATE_BUCKET = 'update-bucket'
_TEST_BQ_DATASET = 'feed-dataset'


@mock.patch.dict(os.environ, {
    'UPDATE_BUCKET': _TEST_UPDATE_BUCKET,
    'BQ_DATASET': _TEST_BQ_DATASET
})
@mock.patch('main._get_current_time_in_utc')
class ImportStorageFileIntoBigQueryTest(unittest.TestCase):

  def test_import_storage_file_drops_event_if_runtime_exceeds_max(
      self, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    long_running_datetime = '2021-06-05T09:16:15.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        long_running_datetime)

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      main.import_storage_file_into_big_query(event, context)

      self.assertIn('Dropping event 12345', mock_logging.output[0])

  @mock.patch.dict(os.environ, {'UPDATE_BUCKET': ''})
  def test_import_storage_file_into_big_query_reports_error_on_missing_update_bucket_env_var(
      self, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with self.assertLogs(level='ERROR') as mock_logging:
      main.import_storage_file_into_big_query(event, context)

      self.assertIn('Update Bucket Environment Variable not found.',
                    mock_logging.output[0])

  def test_import_storage_file_into_big_query_reports_error_on_nonexistent_bucket(
      self, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_storage_client.return_value.get_bucket.side_effect = exceptions.NotFound(
          '404')

      main.import_storage_file_into_big_query(event, context)

      self.assertIn('Bucket update-bucket could not be found.',
                    mock_logging.output[0])

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  def test_import_storage_file_into_big_query_logs_file_import(
      self, _, mock_file_to_import_exists, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(event, context)

      self.assertIn(f'Uploaded Filename: feed-bucket/{test_filename}',
                    mock_stdout.getvalue())

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  def test_import_storage_file_into_big_query_calls_get_bucket(
      self, _, mock_file_to_import_exists, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_get_bucket.return_value.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(event, context)

      mock_get_bucket.assert_called_with(_TEST_UPDATE_BUCKET)

  @mock.patch('main._file_to_import_exists')
  @mock.patch('main._perform_big_query_load')
  def test_import_storage_file_into_big_query_checks_for_eof(
      self, _, mock_file_to_import_exists, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      mock_file_to_import_exists.return_value = True

      main.import_storage_file_into_big_query(event, context)

      mock_get_bucket.get_blob.assert_called_with('EOF')

  def test_import_storage_file_into_big_query_throws_error_if_eof_exists(
      self, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      main.import_storage_file_into_big_query(event, context)

      self.assertIn('An EOF file was found in bucket', mock_logging.output[0])

  def test_import_storage_file_into_big_query_loads_data_to_bq_successfully(
      self, mock_get_current_time_in_utc):
    test_filename = 'feedfile'
    event = {
        'bucket': 'feed-bucket',
        'name': test_filename,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    context = mock.MagicMock()
    context.event_id = '12345'
    context.event_type = 'gcs-event'
    context.timestamp = '2021-06-05T08:16:15.183Z'
    mock_current_datetime = '2021-06-05T08:16:25.183Z'
    mock_get_current_time_in_utc.return_value = iso8601.parse_date(
        mock_current_datetime)

    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client') as mock_bigquery_client:

      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = None
      main.import_storage_file_into_big_query(event, context)
      mock_bigquery_client.return_value.load_table_from_uri.assert_called_with(
          'gs://feed-bucket/feedfile',
          f'{_TEST_BQ_DATASET}.items',
          job_config=mock.ANY)
