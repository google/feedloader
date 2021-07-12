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
import unittest.mock as mock

from absl.testing import parameterized

import main

_TEST_EOF_BUCKET = 'update-bucket'
_TEST_FILENAME = 'EOF'
_TEST_LOCK_BUCKET = 'lock-bucket'
_TEST_LOCK_FILE_NAME = 'EOF.lock'


@mock.patch.dict(os.environ, {
    'LOCK_BUCKET': _TEST_LOCK_BUCKET,
})
class CaclulateProductChangesTest(parameterized.TestCase):

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

      mock_get_bucket.call_args_list[0].assert_called_with(_TEST_EOF_BUCKET)
      mock_get_bucket.call_args_list[1].assert_called_with(_TEST_LOCK_BUCKET)
      mock_get_bucket.return_value.get_blob.assert_called_with(_TEST_FILENAME)
      mock_get_bucket.return_value.copy_blob.assert_called_with(
          mock.ANY, mock.ANY, new_name=_TEST_LOCK_FILE_NAME)
      self.assertIn('Starting diff calculation Cloud Function...',
                    mock_stdout.getvalue())

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

      assert not mock_lock_exists.called
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
