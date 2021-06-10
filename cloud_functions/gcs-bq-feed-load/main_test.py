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
import os
import unittest
import unittest.mock as mock
import iso8601
import main

_TEST_UPDATE_BUCKET = 'update-bucket'


@mock.patch.dict(os.environ, {'UPDATE_BUCKET': _TEST_UPDATE_BUCKET})
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

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_log:
      mock_get_current_time_in_utc.return_value = iso8601.parse_date(
          long_running_datetime)
      main.import_storage_file_into_big_query(event, context)
      self.assertIn('Dropping event', mock_log.output[0])

  def test_import_storage_file_into_big_query_logs_file_import(
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

    with mock.patch('main.storage.Client'), self.assertLogs(
        level='INFO') as mock_log:
      mock_get_current_time_in_utc.return_value = iso8601.parse_date(
          mock_current_datetime)
      main.import_storage_file_into_big_query(event, context)
      self.assertIn(f'Uploaded Filename: {test_filename}', mock_log.output[4])

  def test_import_storage_file_into_big_query_calls_get_bucket(
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

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_current_time_in_utc.return_value = iso8601.parse_date(
          mock_current_datetime)
      main.import_storage_file_into_big_query(event, context)
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_get_bucket.assert_called_with(_TEST_UPDATE_BUCKET)

  def test_import_storage_file_into_big_query_checks_for_eof(
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

    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_get_current_time_in_utc.return_value = iso8601.parse_date(
          mock_current_datetime)
      main.import_storage_file_into_big_query(event, context)
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.assert_called_with('EOF')

  def test_import_storage_file_into_big_query_logs_error_if_eof_exists(
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

    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_log:
      mock_get_current_time_in_utc.return_value = iso8601.parse_date(
          mock_current_datetime)
      main.import_storage_file_into_big_query(event, context)
      mock_get_bucket = mock_storage_client.return_value.get_bucket.return_value
      mock_get_bucket.get_blob.return_value = mock.MagicMock()
      self.assertIn(f'EOF file was found in bucket: {_TEST_UPDATE_BUCKET}',
                    mock_log.output[0])
