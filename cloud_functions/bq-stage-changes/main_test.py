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
import datetime
import io
import os
import types
import unittest.mock as mock

from absl.testing import parameterized
from google.api_core import exceptions

import iso8601
import main

_TEST_BQ_DATASET = 'dataset'
_TEST_COMPLETED_FILES_BUCKET = 'completed-bucket'
_TEST_DELETES_TABLE_NAME = main._DELETES_TABLE_NAME
_TEST_EOF_BUCKET = 'update-bucket'
_TEST_EXPIRATIONS_TABLE_NAME = main._EXPIRATION_TABLE_NAME
_TEST_EXPIRATION_THRESHOLD = '25'
_TEST_FEED_BUCKET = 'feed-bucket'
_TEST_FILENAME = 'EOF'
_TEST_GAE_ACTIONS = main._GAE_ACTIONS
_TEST_GCP_PROJECT_ID = 'test-project'
_TEST_ITEMS_TABLE = main._ITEMS_TABLE_NAME
_TEST_ITEMS_TO_DELETE_TABLE = main._ITEMS_TO_DELETE_TABLE_NAME
_TEST_ITEMS_TO_PREVENT_EXPIRING_TABLE = (
    main._ITEMS_TO_PREVENT_EXPIRING_TABLE_NAME)
_TEST_ITEMS_TO_UPSERT_TABLE = main._ITEMS_TO_UPSERT_TABLE_NAME
_TEST_STREAMING_ITEMS_TABLE = main._STREAMING_ITEMS_TABLE_NAME
_TEST_ITEMS_TABLE_EXPIRATION_DURATION = main._ITEMS_TABLE_EXPIRATION_DURATION
_TEST_LOCK_BUCKET = 'lock-bucket'
_TEST_LOCK_FILE_NAME = main._LOCK_FILE_NAME
_TEST_MERCHANT_ID_SQL = f'{main._MERCHANT_ID_COLUMN},'
_TEST_QUERY = 'SELECT * from test_items_table'
_TEST_RETRIGGER_BUCKET = 'retrigger-bucket'
_TEST_STREAMING_ITEMS_TABLE_NAME = main._STREAMING_ITEMS_TABLE_NAME
_TEST_TIMESTAMP = '2021-06-05T08:16:25.183Z'
_TEST_TIMEZONE_UTC_OFFSET = '+09:00'
_TEST_UPSERTS_TABLE_NAME = main._UPSERTS_TABLE_NAME
_TEST_WRITE_DISPOSITION = main._WRITE_DISPOSITION


@mock.patch.dict(
    os.environ, {
        'BQ_DATASET': _TEST_BQ_DATASET,
        'COMPLETED_FILES_BUCKET': _TEST_COMPLETED_FILES_BUCKET,
        'EXPIRATION_THRESHOLD': _TEST_EXPIRATION_THRESHOLD,
        'FEED_BUCKET': _TEST_FEED_BUCKET,
        'GCP_PROJECT': _TEST_GCP_PROJECT_ID,
        'LOCK_BUCKET': _TEST_LOCK_BUCKET,
        'RETRIGGER_BUCKET': _TEST_RETRIGGER_BUCKET,
        'TIMEZONE_UTC_OFFSET': _TEST_TIMEZONE_UTC_OFFSET,
    })
@mock.patch(
    'main._get_current_time_in_utc',
    return_value=iso8601.parse_date(_TEST_TIMESTAMP))
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
  @mock.patch('main._set_table_expiration_date')
  @mock.patch('main._clean_up')
  @mock.patch('main._archive_folder')
  def test_import_calculate_product_changes_locks_eof_file_when_no_lock_exists(
      self, mock_archive_folder, mock_clean_up, mock_set_table_expiration_date,
      mock_lock_exists, _):
    del mock_clean_up, mock_set_table_expiration_date  # unused by this test.
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_lock_exists.return_value = False
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_archive_folder.return_value = True

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
      self, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      bad_filename = 'bad_file'
      self.event['name'] = bad_filename

      main.calculate_product_changes(self.event, self.context)

      mock_lock_exists.assert_not_called()
      self.assertIn(f'File {bad_filename} was not an EOF! Exiting Function...',
                    mock_logging.output[0])

  @mock.patch('main._lock_exists')
  @mock.patch('main._set_table_expiration_date')
  def test_import_calculate_product_changes_errors_out_when_trigger_file_is_not_empty(
      self, mock_set_table_expiration_date, mock_lock_exists, _):
    del mock_set_table_expiration_date  # unused by this test.
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
      self, mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), self.assertLogs(
        level='ERROR') as mock_logging:
      mock_lock_exists.return_value = True

      main.calculate_product_changes(self.event, self.context)

      mock_lock_eof.assert_not_called()
      self.assertIn('An EOF.lock file was found', mock_logging.output[0])

  @mock.patch('main._lock_exists')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._set_table_expiration_date')
  @mock.patch('main._table_exists')
  def test_calculate_product_changes_checks_existence_of_required_tables(
      self, mock_table_exists, mock_set_table_expiration_date,
      mock_cleanup_completed_filenames, mock_ensure_all_files_were_imported,
      mock_lock_exists, _):
    del mock_set_table_expiration_date  # unused by this test.
    with mock.patch('main.storage.Client'), mock.patch('main.bigquery.Client'):
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      fully_qualified_items_table = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')
      fully_qualified_items_to_delete_table_name = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.'
          f'{_TEST_ITEMS_TO_DELETE_TABLE}')
      fully_qualified_items_to_prevent_expiring_table_name = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.'
          f'{_TEST_ITEMS_TO_PREVENT_EXPIRING_TABLE}')
      fully_qualified_items_to_upsert_table_name = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.'
          f'{_TEST_ITEMS_TO_UPSERT_TABLE}')
      fully_qualified_streaming_items_table_name = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.'
          f'{_TEST_STREAMING_ITEMS_TABLE}')
      mock_table_exists.side_effect = [True] * 5

      main.calculate_product_changes(self.event, self.context)

      calls = [
          mock.call(mock.ANY, fully_qualified_items_table),
          mock.call(mock.ANY, fully_qualified_items_to_delete_table_name),
          mock.call(mock.ANY,
                    fully_qualified_items_to_prevent_expiring_table_name),
          mock.call(mock.ANY, fully_qualified_items_to_upsert_table_name),
          mock.call(mock.ANY, fully_qualified_streaming_items_table_name)
      ]
      mock_table_exists.assert_has_calls(calls)

  @mock.patch('main._lock_exists')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._set_table_expiration_date')
  @mock.patch('main._table_exists')
  @mock.patch('main._clean_up')
  def test_calculate_product_changes_logs_error_when_any_required_table_is_missing(
      self, mock_clean_up, mock_table_exists, mock_set_table_expiration_date,
      mock_cleanup_completed_filenames, mock_ensure_all_files_were_imported,
      mock_lock_exists, _):
    del mock_set_table_expiration_date  # unused by this test.
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client'), self.assertLogs(level='ERROR') as mock_logging:
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_table_exists.return_value = False
      fully_qualified_items_table = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('One or more necessary tables are missing.',
                    mock_logging.output[0])
      mock_clean_up.assert_called_with(mock.ANY, mock.ANY, _TEST_LOCK_BUCKET,
                                       fully_qualified_items_table)

  def test_table_exists_returns_false_when_table_is_missing(self, _):
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client') as mock_bigquery_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_get_table = mock_bigquery_client.get_table
      mock_get_table.side_effect = exceptions.NotFound('404')
      fully_qualified_items_table = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')

      result = main._table_exists(mock_bigquery_client,
                                  fully_qualified_items_table)

      mock_get_table.assert_called_with(fully_qualified_items_table)
      self.assertIn(f'Table {fully_qualified_items_table} must exist',
                    mock_logging.output[0])
      self.assertFalse(result)

  def test_table_exists_returns_true_when_table_exists(self, _):
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client') as mock_bigquery_client:
      mock_get_table = mock_bigquery_client.get_table
      fully_qualified_items_table = (
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')

      result = main._table_exists(mock_bigquery_client,
                                  fully_qualified_items_table)

      mock_get_table.assert_called_with(fully_qualified_items_table)
      self.assertTrue(result)

  @mock.patch('main._lock_exists')
  @mock.patch('main._trigger_reupload_of_missing_feed_files')
  def test_ensure_all_files_were_imported_calls_retry_function_if_any_missing_files_detected(
      self, mock_trigger_reupload_function, mock_lock_exists, _):
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

      self.assertEqual(_TEST_FEED_BUCKET,
                       mock_list_blobs.call_args_list[0].args[0])
      mock_trigger_reupload_function.assert_called()

  @mock.patch('main._lock_exists')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._set_table_expiration_date')
  @mock.patch('main._clean_up')
  @mock.patch('main._archive_folder')
  @mock.patch('main._table_exists')
  def test_ensure_all_files_were_imported_returns_true_if_attempted_and_completed_file_sets_match(
      self, mock_table_exists, mock_archive_folder, mock_clean_up,
      mock_set_table_expiration_date, mock_cleanup_completed_filenames,
      mock_lock_exists, _):
    del mock_clean_up, mock_set_table_expiration_date  # unused by this test.
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_table_exists.return_value = True
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
      mock_archive_folder.return_value = True

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('All the feeds were loaded', mock_stdout.getvalue())

  @mock.patch('main._lock_exists')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._set_table_expiration_date')
  def test_cleanup_completed_filenames_logs_error_if_it_returned_false(
      self, mock_set_table_expiration_date, mock_cleanup_completed_filenames,
      mock_lock_exists, _):
    del mock_set_table_expiration_date  # unused by this test.
    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertLogs(
            level='ERROR') as mock_logging:
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = False
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

      self.assertIn('Cleanup completed filenames failed.',
                    mock_logging.output[0])

  @mock.patch('main._lock_exists')
  def test_trigger_reupload_of_missing_feed_files_uploads_filenames_string_to_retrigger_bucket(
      self, mock_lock_exists, _):
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
  @mock.patch('main._clean_up')
  def test_ensure_all_files_were_imported_returns_logs_error_when_attempted_files_is_empty(
      self, mock_clean_up, mock_lock_exists, _):
    del mock_clean_up  # unused by this test.
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
  @mock.patch('main._clean_up')
  def test_ensure_all_files_were_imported_calls_clean_up_when_attempted_files_is_empty(
      self, mock_clean_up, mock_lock_exists, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
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

      mock_clean_up.assert_called_with(
          mock.ANY, mock.ANY, _TEST_LOCK_BUCKET,
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')

  @mock.patch('main._lock_exists')
  @mock.patch('main._clean_up')
  def test_ensure_all_files_were_imported_returns_logs_error_when_completed_files_is_empty(
      self, mock_clean_up, mock_lock_exists, _):
    del mock_clean_up  # unused by this test.
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

  @mock.patch('main._lock_exists')
  @mock.patch('main._clean_up')
  def test_ensure_all_files_were_imported_calls_clean_up_when_completed_files_is_empty(
      self, mock_clean_up, mock_lock_exists, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
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

      mock_clean_up.assert_called_with(
          mock.ANY, mock.ANY, _TEST_LOCK_BUCKET,
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')

  @mock.patch('main._lock_exists')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._table_exists')
  def test_set_table_expiration_date_sets_table_expiration(
      self, mock_table_exists, mock_cleanup_completed_filenames,
      mock_ensure_all_files_were_imported, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client') as mock_bigquery_client:
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_table_exists.return_value = True
      test_table_with_expiration = (
          types.SimpleNamespace(expires=datetime.datetime.now()))
      mock_bigquery_client.return_value.get_table.return_value = (
          test_table_with_expiration)

      expiration_duration = datetime.timedelta(
          milliseconds=_TEST_ITEMS_TABLE_EXPIRATION_DURATION)
      expected_expiration = iso8601.parse_date(
          '2021-06-05T08:16:25.183Z') + expiration_duration
      expected_table_with_expiration = (
          types.SimpleNamespace(expires=expected_expiration))

      main.calculate_product_changes(self.event, self.context)

      mock_bigquery_client.return_value.get_table.assert_called_with(
          f'{_TEST_GCP_PROJECT_ID}.{_TEST_BQ_DATASET}.{_TEST_ITEMS_TABLE}')
      mock_bigquery_client.return_value.update_table.assert_called_with(
          expected_table_with_expiration, ['expires'])

  @mock.patch('main._lock_exists')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._set_table_expiration_date')
  @mock.patch('main._archive_folder')
  @mock.patch('main._table_exists')
  @mock.patch('main._parse_bigquery_config')
  @mock.patch('main._run_materialize_job')
  @mock.patch('main._count_changes')
  def test_cleanup_completed_filenames_is_called_if_ensure_all_files_were_imported_was_successful(
      self, mock_count_changes, mock_run_materialize_job,
      mock_parse_bigquery_config, mock_table_exists, mock_archive_folder,
      mock_set_table_expiration_date, mock_cleanup_completed_filenames,
      mock_lock_exists, _):
    # unused by this test.
    del mock_run_materialize_job, mock_set_table_expiration_date
    with mock.patch('main.storage.Client') as mock_storage_client:
      mock_table_exists.return_value = True
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
      mock_archive_folder.return_value = True
      mock_parse_bigquery_config.return_value = (_TEST_QUERY, '')
      mock_count_changes.side_effect = [0, 0, 0]

      main.calculate_product_changes(self.event, self.context)

      mock_cleanup_completed_filenames.assert_called()

  def test_delete_completed_file_deletes_file_from_completed_bucket(self, _):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main._COMPLETED_FILES_BUCKET', _TEST_COMPLETED_FILES_BUCKET):
      test_bucket_file_to_delete = 'test_feed_file.txt'
      mock_get_bucket = mock_storage_client.return_value.get_bucket

      main._delete_completed_file(test_bucket_file_to_delete)

      mock_get_bucket.assert_called_with(_TEST_COMPLETED_FILES_BUCKET)
      mock_get_bucket.return_value.delete_blob.assert_called_with(
          test_bucket_file_to_delete)

  def test_delete_completed_file_logs_error_on_blob_not_found(self, _):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main._COMPLETED_FILES_BUCKET',
        _TEST_COMPLETED_FILES_BUCKET), self.assertLogs(
            level='ERROR') as mock_logging:
      test_bucket_file_to_delete = 'test_feed_file.txt'
      mock_get_bucket = mock_storage_client.return_value.get_bucket
      mock_get_bucket.return_value.delete_blob.side_effect = (
          exceptions.NotFound('404'))

      main._delete_completed_file(test_bucket_file_to_delete)

      mock_get_bucket.assert_called_with(_TEST_COMPLETED_FILES_BUCKET)
      mock_get_bucket.return_value.delete_blob.assert_called_with(
          test_bucket_file_to_delete)
      self.assertIn(
          f'Failed to delete {test_bucket_file_to_delete} in '
          'f{_TEST_COMPLETED_FILES_BUCKET}.', mock_logging.output[0])

  def test_archive_folder_calls_rename_blob(self, _):
    with mock.patch('main.storage.Client') as mock_storage_client:
      test_file_for_renaming = types.SimpleNamespace(name='file1.txt')
      mock_list_blobs = mock_storage_client.list_blobs
      mock_list_blobs.return_value = [test_file_for_renaming]
      mock_get_bucket = mock_storage_client.get_bucket
      expected_archive_destination = 'archive/2021_06_05_08_16_AM/file1.txt'

      main._archive_folder(mock_storage_client, _TEST_FEED_BUCKET)

      mock_get_bucket.assert_called_with(_TEST_FEED_BUCKET)
      mock_get_bucket.return_value.rename_blob.assert_called_with(
          test_file_for_renaming, expected_archive_destination)

  def test_archive_folder_throws_exception_if_rename_blob_did_not_return_blob(
      self, _):
    with mock.patch(
        'main.storage.Client') as mock_storage_client, self.assertRaises(
            exceptions.GoogleAPICallError):
      test_file_for_renaming = types.SimpleNamespace(name='file1.txt')
      mock_list_blobs = mock_storage_client.list_blobs
      mock_list_blobs.return_value = [test_file_for_renaming]
      mock_get_bucket = mock_storage_client.get_bucket
      mock_get_bucket.return_value.rename_blob.return_value = None

      main._archive_folder(mock_storage_client, _TEST_FEED_BUCKET)

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  def test_calculate_product_changes_raises_upon_archive_exception(
      self, mock_cleanup_completed_filenames,
      mock_ensure_all_files_were_imported, mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client'), self.assertRaises(exceptions.NotFound):
      del mock_lock_eof  # unused by this test.
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_storage_client.return_value.get_bucket.side_effect = (
          exceptions.NotFound('Bucket not found!'))

      main.calculate_product_changes(self.event, self.context)

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._clean_up')
  def test_calculate_product_changes_calls_clean_up_upon_archive_exception(
      self, mock_clean_up, mock_cleanup_completed_filenames,
      mock_ensure_all_files_were_imported, mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client') as mock_storage_client, mock.patch(
        'main.bigquery.Client'):
      del mock_lock_eof  # unused by this test.
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_storage_client.return_value.get_bucket.side_effect = (
          exceptions.NotFound('Bucket not found!'))

      main.calculate_product_changes(self.event, self.context)

      mock_clean_up.assert_called()

  def test_parse_config_returns_converted_config_and_mc_column(self, _):
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
    expected_query_result = ('IFNULL(CAST(Items.google_merchant_id AS STRING), '
                             '\'NULL\'), IFNULL(CAST(Items.title AS STRING), '
                             '\'NULL\')')

    with mock.patch('builtins.open', mock.mock_open(
        read_data='')) as mock_file, mock.patch('json.load') as mock_json_load:
      mock_json_load.return_value = test_config

      (query_result, mc_column_result) = main._parse_bigquery_config()

      mock_file.assert_called_with('config.json')
      self.assertEqual(_TEST_MERCHANT_ID_SQL, mc_column_result)
      self.assertEqual(expected_query_result, query_result)

  def test_parse_config_raises_on_json_load_failure(self, _):
    with mock.patch('builtins.open', mock.mock_open(read_data='')), mock.patch(
        'json.load') as mock_json_load, self.assertRaises(
            exceptions.GoogleAPICallError):
      mock_json_load.return_value = None

      main._parse_bigquery_config()

  def test_run_materialize_job_calls_bigquery(self, _):
    with mock.patch('main.bigquery.Client') as mock_bigquery_client:
      test_destination_table = 'streaming_items'
      test_write_disposition = 'WRITE_APPEND'

      main._run_materialize_job(mock_bigquery_client, _TEST_BQ_DATASET,
                                test_destination_table, _TEST_GCP_PROJECT_ID,
                                _TEST_QUERY, test_write_disposition)

      mock_bigquery_client.query.assert_called_with(
          _TEST_QUERY, job_config=mock.ANY)

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._archive_folder')
  @mock.patch('main._parse_bigquery_config')
  @mock.patch('main._run_materialize_job')
  @mock.patch('main._clean_up')
  def test_calculate_product_changes_catches_and_logs_materialize_exception(
      self, mock_clean_up, mock_run_materialize_job, mock_parse_bigquery_config,
      mock_archive_folder, mock_cleanup_completed_filenames,
      mock_ensure_all_files_were_imported, mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client'), self.assertLogs(level='ERROR') as mock_logging:
      del mock_archive_folder, mock_lock_eof  # unused by this test.
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_parse_bigquery_config.return_value = ('', '')
      mock_run_materialize_job.side_effect = exceptions.GoogleAPICallError(
          'Bigquery Query Failed.')

      main.calculate_product_changes(self.event, self.context)

      mock_clean_up.assert_called()
      self.assertIn('Bigquery Query Failed.', mock_logging.output[0])

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._archive_folder')
  @mock.patch('main._parse_bigquery_config')
  @mock.patch('main._run_materialize_job')
  @mock.patch('main._count_changes')
  def test_calculate_product_changes_calls_run_materialize_job_for_required_tables(
      self, mock_count_changes, mock_run_materialize_job,
      mock_parse_bigquery_config, mock_archive_folder,
      mock_cleanup_completed_filenames, mock_ensure_all_files_were_imported,
      mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), mock.patch('main.bigquery.Client'):
      del mock_archive_folder, mock_lock_eof  # unused by this test.
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_parse_bigquery_config.return_value = ('', '')
      mock_count_changes.side_effect = [0, 0, 0]

      expected_run_materialize_job_calls = [
          mock.call(mock.ANY, mock.ANY, _TEST_STREAMING_ITEMS_TABLE_NAME,
                    mock.ANY, mock.ANY,
                    _TEST_WRITE_DISPOSITION.WRITE_TRUNCATE.name),
          mock.call(mock.ANY, mock.ANY, _TEST_DELETES_TABLE_NAME, mock.ANY,
                    mock.ANY, _TEST_WRITE_DISPOSITION.WRITE_APPEND.name),
          mock.call(mock.ANY, mock.ANY, _TEST_UPSERTS_TABLE_NAME, mock.ANY,
                    mock.ANY, _TEST_WRITE_DISPOSITION.WRITE_TRUNCATE.name),
          mock.call(mock.ANY, mock.ANY, _TEST_UPSERTS_TABLE_NAME, mock.ANY,
                    mock.ANY, _TEST_WRITE_DISPOSITION.WRITE_APPEND.name),
          mock.call(mock.ANY, mock.ANY, _TEST_EXPIRATIONS_TABLE_NAME, mock.ANY,
                    mock.ANY, _TEST_WRITE_DISPOSITION.WRITE_TRUNCATE.name),
      ]

      main.calculate_product_changes(self.event, self.context)

      for i, call in enumerate(expected_run_materialize_job_calls):
        mock_run_materialize_job.call_args_list[i].assert_has_calls(call)

  def test_count_changes_calls_bigquery_and_returns_count(self, _):
    with mock.patch('main.bigquery.Client') as mock_bigquery_client, mock.patch(
        'sys.stdout', new_callable=io.StringIO) as mock_stdout:
      test_count_deletes_query = 'SELECT COUNT(*) FROM items_to_delete'
      test_action = _TEST_GAE_ACTIONS.delete.name
      test_delete_count = 1
      mock_query_job = mock_bigquery_client.query.return_value
      mock_query_job.result.return_value = [
          type('', (object,), {'f0_': test_delete_count})()
      ]

      main._count_changes(mock_bigquery_client, test_count_deletes_query,
                          test_action)

      mock_bigquery_client.query.assert_called_with(test_count_deletes_query)
      self.assertIn(
          f'Number of rows to {_TEST_GAE_ACTIONS.delete.name} in this run: '
          'f{test_delete_count}', mock_stdout.getvalue())

  def test_count_changes_returns_negative_one_if_results_was_empty(self, _):
    with mock.patch('main.bigquery.Client') as mock_bigquery_client:
      test_count_deletes_query = 'SELECT COUNT(*) FROM items_to_delete'
      test_action = _TEST_GAE_ACTIONS.delete.name
      mock_query_job = mock_bigquery_client.query.return_value
      mock_query_job.result.return_value = []

      count_changes_result = main._count_changes(mock_bigquery_client,
                                                 test_count_deletes_query,
                                                 test_action)

      self.assertEqual(-1, count_changes_result)

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._archive_folder')
  @mock.patch('main._parse_bigquery_config')
  @mock.patch('main._run_materialize_job')
  @mock.patch('main._count_changes')
  def test_calculate_product_changes_counts_deletes_upserts_and_expirations(
      self, mock_count_changes, mock_run_materialize_job,
      mock_parse_bigquery_config, mock_archive_folder,
      mock_cleanup_completed_filenames, mock_ensure_all_files_were_imported,
      mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), mock.patch('main.bigquery.Client'):
      # unused by this test.
      del mock_run_materialize_job, mock_archive_folder, mock_lock_eof
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_parse_bigquery_config.return_value = ('', '')
      mock_count_changes.side_effect = [100, 1000, 10]

      expected_count_changes_calls = [
          mock.call(mock.ANY, mock.ANY, mock.ANY, mock.ANY,
                    _TEST_GAE_ACTIONS.delete.name),
          mock.call(mock.ANY, mock.ANY, mock.ANY, mock.ANY,
                    _TEST_GAE_ACTIONS.upsert.name),
          mock.call(mock.ANY, mock.ANY, mock.ANY, mock.ANY,
                    _TEST_GAE_ACTIONS.prevent_expiring.name),
      ]

      main.calculate_product_changes(self.event, self.context)

      for i, call in enumerate(expected_count_changes_calls):
        mock_count_changes.call_args_list[i].assert_has_calls(call)

  @mock.patch('main._lock_exists')
  @mock.patch('main._lock_eof')
  @mock.patch('main._ensure_all_files_were_imported')
  @mock.patch('main._cleanup_completed_filenames')
  @mock.patch('main._archive_folder')
  @mock.patch('main._parse_bigquery_config')
  @mock.patch('main._run_materialize_job')
  @mock.patch('main._count_changes')
  def test_calculate_product_changes_logs_errors_if_count_changes_fails(
      self, mock_count_changes, mock_run_materialize_job,
      mock_parse_bigquery_config, mock_archive_folder,
      mock_cleanup_completed_filenames, mock_ensure_all_files_were_imported,
      mock_lock_eof, mock_lock_exists, _):
    with mock.patch('main.storage.Client'), mock.patch(
        'main.bigquery.Client'), self.assertLogs(level='ERROR') as mock_logging:
      # unused by this test.
      del mock_run_materialize_job, mock_archive_folder, mock_lock_eof
      mock_lock_exists.return_value = False
      mock_cleanup_completed_filenames.return_value = True
      mock_ensure_all_files_were_imported.return_value = (True, [])
      mock_parse_bigquery_config.return_value = ('', '')
      mock_count_changes.side_effect = [-1, -1, -1]

      main.calculate_product_changes(self.event, self.context)

      self.assertIn('Delete count job failed.', mock_logging.output[0])
      self.assertIn('Upsert count failed.', mock_logging.output[1])
      self.assertIn('Expiring count failed.', mock_logging.output[2])
