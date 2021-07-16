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

"""Unit tests for Calculate Product Changes Cloud Function main.py."""
import io
import os
import types
import unittest.mock as mock

from absl.testing import parameterized

import main

_TEST_COMPLETED_FILES_BUCKET = 'completed-bucket'
_TEST_EOF_BUCKET = 'update-bucket'
_TEST_FEED_BUCKET = 'feed-bucket'
_TEST_FILENAME = 'EOF'
_TEST_LOCK_BUCKET = 'lock-bucket'
_TEST_LOCK_FILE_NAME = 'EOF.lock'
_TEST_RETRIGGER_BUCKET = 'retrigger-bucket'


@mock.patch.dict(
    os.environ, {
        'COMPLETED_FILES_BUCKET': _TEST_COMPLETED_FILES_BUCKET,
        'FEED_BUCKET': _TEST_FEED_BUCKET,
        'LOCK_BUCKET': _TEST_LOCK_BUCKET,
        'RETRIGGER_BUCKET': _TEST_RETRIGGER_BUCKET,
    })
class CalculateProductChangesTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.event = {
        'bucket': _TEST_EOF_BUCKET,
        'name': _TEST_FILENAME,
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0',
        'size': '0',
    }
    self.context = mock.MagicMock()
    self.context.event_id = '12345'
    self.context.event_type = 'gcs-event'
    self.context.timestamp = '2021-06-05T08:16:15.183Z'

  @mock.patch('main._lock_exists')
  def test_import_calculate_product_changes_locks_eof_file_when_no_lock_exists(
      self, mock_lock_exists):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_lock_exists.return_value = False
      mock_get_bucket = mock_storage_client.return_value.get_bucket

      main.calculate_product_changes(self.event, self.context)

      self.assertEqual(mock_get_bucket.call_args_list[0].args[0],
                       _TEST_EOF_BUCKET)
      self.assertEqual(mock_get_bucket.call_args_list[1].args[0],
                       _TEST_LOCK_BUCKET)
      mock_get_bucket.return_value.get_blob.assert_called_with(_TEST_FILENAME)
      mock_get_bucket.return_value.copy_blob.assert_called_with(
          mock.ANY, mock.ANY, new_name=_TEST_LOCK_FILE_NAME)
      self.assertIn('Empty EOF file detected.', mock_stdout.getvalue())

  @mock.patch('main._lock_exists')
  def test_import_calculate_product_changes_errors_out_when_trigger_file_is_not_eof(
      self, mock_lock_exists):
    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      bad_filename = 'bad_file'
      self.event['name'] = bad_filename

      main.calculate_product_changes(self.event, self.context)

      mock_lock_exists.assert_not_called()
      self.assertIn(f'File {bad_filename} was not an EOF! Exiting Function...',
                    mock_logging.output[0])

  @mock.patch('main._lock_exists')
  def test_import_calculate_product_changes_errors_out_when_trigger_file_is_not_empty(
      self, mock_lock_exists):
    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      self.event['size'] = '1'

      main.calculate_product_changes(self.event, self.context)

      mock_lock_exists.assert_not_called()
      self.assertIn(f'File {_TEST_FILENAME} was not empty! Exiting Function...',
                    mock_logging.output[0])

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  def test_import_calculate_product_changes_errors_out_when_lock_exists(
      self, mock_lock_eof, mock_lock_exists):
    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      mock_lock_exists.return_value = True

      main.calculate_product_changes(self.event, self.context)

      mock_lock_eof.assert_not_called()
      self.assertIn('An EOF.lock file was found', mock_logging.output[0])

  @mock.patch('main._lock_exists')
  @mock.patch('main._trigger_reupload_of_missing_feed_files')
  def test_ensure_all_files_were_imported_calls_retry_function_if_any_missing_files_detected(
      self, mock_trigger_reupload_function, mock_lock_exists):
    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_lock_exists.return_value = False
      test_attempted_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file2'),
          types.SimpleNamespace(name='file3')
      ])
      test_completed_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file3')
      ])
      mock_list_blobs = mock_storage_client.return_value.list_blobs
      mock_list_blobs.side_effect = [test_attempted_files, test_completed_files]

      main.calculate_product_changes(self.event, self.context)

      self.assertEqual(mock_list_blobs.call_args_list[0].args[0],
                       _TEST_FEED_BUCKET)
      self.assertEqual(mock_list_blobs.call_args_list[1].args[0],
                       _TEST_COMPLETED_FILES_BUCKET)
      mock_trigger_reupload_function.assert_called()

  @mock.patch('main._lock_exists')
  def test_ensure_all_files_were_imported_returns_true_if_attempted_and_completed_file_sets_match(
      self, mock_lock_exists):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_lock_exists.return_value = False
      test_attempted_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file2'),
          types.SimpleNamespace(name='file3')
      ])
      test_completed_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file3'),
          types.SimpleNamespace(name='file2')
      ])
      mock_storage_client.return_value.list_blobs.side_effect = [
          test_attempted_files, test_completed_files
      ]

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('All the feeds were loaded.', mock_stdout.getvalue())

  @mock.patch('main._lock_exists')
  def test_trigger_reupload_of_missing_feed_files_uploads_filenames_string_to_retrigger_bucket(
      self, mock_lock_exists):
    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_lock_exists.return_value = False
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_upload_from_string = (
          mock_get_bucket.return_value.blob.return_value.upload_from_string)
      test_attempted_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file2'),
          types.SimpleNamespace(name='file3'),
          types.SimpleNamespace(name='file4')
      ])
      test_completed_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file3')
      ])
      mock_storage_client.return_value.list_blobs.side_effect = [
          test_attempted_files, test_completed_files
      ]

      main.calculate_product_changes(self.event, self.context)

      self.assertEqual(mock_get_bucket.call_args_list[2].args[0],
                       _TEST_RETRIGGER_BUCKET)
      self.assertEqual(mock_upload_from_string.call_args_list[0].args[0],
                       'file2\nfile4')

  @mock.patch('main._lock_exists')
  def test_ensure_all_files_were_imported_returns_logs_error_when_attempted_files_is_empty(
      self, mock_lock_exists):
    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_lock_exists.return_value = False
      test_attempted_files = iter([])
      test_completed_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file3'),
          types.SimpleNamespace(name='file2')
      ])
      mock_storage_client.return_value.list_blobs.side_effect = [
          test_attempted_files, test_completed_files
      ]

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('Attempted feeds retrieval failed', mock_logging.output[0])

  @mock.patch('main._lock_exists')
  def test_ensure_all_files_were_imported_returns_logs_error_when_completed_files_is_empty(
      self, mock_lock_exists):
    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_lock_exists.return_value = False
      test_attempted_files = iter([
          types.SimpleNamespace(name='file1'),
          types.SimpleNamespace(name='file3'),
          types.SimpleNamespace(name='file2')
      ])
      test_completed_files = iter([])
      mock_storage_client.return_value.list_blobs.side_effect = [
          test_attempted_files, test_completed_files
      ]

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('Completed filenames retrieval failed',
                    mock_logging.output[0])
