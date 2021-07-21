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

"""CF that triggers on GCS bucket upload to calculate product diffs."""
import datetime
import logging
import os
from typing import Any, Dict, List, Tuple

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage
import pytz

_BUCKET_DELIMITER = '/'
_ITEMS_TABLE_EXPIRATION_DURATION = 43200000  # 12 hours.
_ITEMS_TABLE_NAME = 'items'
_LOCK_FILE_NAME = 'EOF.lock'


def calculate_product_changes(
    event: Dict[str, Any], context: 'google.cloud.functions.Context') -> None:
  """Cloud Function ("CF") triggered by a Cloud Storage ("GCS") bucket upload.

     This CF calculates a diff between the last load of feeds and this one,
     then reports the number of necessary updates, deletes, and expirations to
     App Engine via Task Queue.

  Args:
      event:  The dictionary with data specific to this type of event. The
        `data` field contains a description of the event in the Cloud Storage
        `object` format described here:
        https://cloud.google.com/storage/docs/json_api/v1/objects#resource
      context: Metadata of triggering event.

  Raises:
    RuntimeError: A dependency was not found, requiring this CF to exit.

  Returns:
      None; the output is written to Cloud logging.
  """
  del context

  if _eof_is_invalid(event):
    return

  bigquery_client = bigquery.Client()
  storage_client = storage.Client()
  feed_bucket = os.environ.get('FEED_BUCKET')
  completed_files_bucket = os.environ.get('COMPLETED_FILES_BUCKET')

  # If the CF was not triggered by a retry, then handle the locking routine.
  if event['name'] != 'EOF.retry':
    if _lock_exists(storage_client):
      logging.error(
          exceptions.AlreadyExists((
              'An EOF.lock file was found, indicating that this CF is still running.'
              'Exiting Function...')))
      return
    # Lock the EOF file at this point to prevent concurrent runs.
    _lock_eof(storage_client, event['bucket'], event['name'])

  print('Empty EOF file detected. Checking files were imported successfully...')

  import_successful, missing_files = (
      _ensure_all_files_were_imported(storage_client, feed_bucket,
                                      completed_files_bucket))

  if not import_successful:
    _trigger_reupload_of_missing_feed_files(storage_client, missing_files)
    return
  print('All the feeds were loaded. Starting calculate_product_changes...')

  fully_qualified_items_table = f'{os.environ.get("GCP_PROJECT")}.{os.environ.get("BQ_DATASET")}.{_ITEMS_TABLE_NAME}'
  _set_table_expiration_date(bigquery_client, fully_qualified_items_table,
                             _ITEMS_TABLE_EXPIRATION_DURATION)


def _eof_is_invalid(event: Dict[str, Any]) -> bool:
  """Checks if the file that triggered this CF was an empty EOF file."""
  if event['name'] != 'EOF' and event['name'] != 'EOF.retry':
    logging.error(
        exceptions.BadRequest(
            f"File {event['name']} was not an EOF! Exiting Function..."))
    return True
  elif int(event['size']) != 0:
    logging.error(
        exceptions.BadRequest(
            f"File {event['name']} was not empty! Exiting Function..."))
    return True
  return False


def _lock_exists(storage_client: storage.client.Client) -> bool:
  """Helper method that returns True if EOF.lock exists, otherwise False."""
  eof_lock_bucket = storage_client.get_bucket(os.environ.get('LOCK_BUCKET'))
  return storage.Blob(
      bucket=eof_lock_bucket, name=_LOCK_FILE_NAME).exists(storage_client)


def _lock_eof(storage_client: storage.client.Client, eof_bucket_name: str,
              eof_filename: str) -> None:
  """Helper function that sets the EOF to a "locked" state."""
  eof_bucket = storage_client.get_bucket(eof_bucket_name)
  eof_blob = eof_bucket.get_blob(eof_filename)

  # Strip out the bucket prefix in case the user set their env var with one.
  lock_bucket_name_without_prefix = os.environ.get('LOCK_BUCKET').replace(
      'gs://', '')
  lock_destination = storage_client.get_bucket(lock_bucket_name_without_prefix)
  eof_bucket.copy_blob(eof_blob, lock_destination, new_name=_LOCK_FILE_NAME)
  eof_blob.delete()


def _ensure_all_files_were_imported(
    storage_client: storage.client.Client, feed_bucket: str,
    completed_files_bucket: str) -> Tuple[bool, List[str]]:
  """Helper function that checks attempted feeds against expected filenames."""
  attempted_feed_files_iterator = storage_client.list_blobs(
      feed_bucket, delimiter=_BUCKET_DELIMITER)
  attempted_feed_files = list(attempted_feed_files_iterator)
  if not attempted_feed_files:
    logging.error(
        exceptions.NotFound(
            'Attempted feeds retrieval failed, or no files are in the bucket.'))
    return False, []
  attempted_filenames = [feed.name for feed in attempted_feed_files]

  completed_feed_files_iterator = storage_client.list_blobs(
      completed_files_bucket)
  completed_feed_files = list(completed_feed_files_iterator)
  if not completed_feed_files:
    logging.error(
        exceptions.NotFound(
            'Completed filenames retrieval failed, or no files in the bucket.'))
    return False, []
  completed_filenames = set(feed.name for feed in completed_feed_files)

  # Compare the set of attempted files to the set of known completed files to
  # find out which ones were missed during the BigQuery import.
  missing_files = [
      filename for filename in attempted_filenames
      if filename not in completed_filenames
  ]
  if missing_files:
    return False, missing_files

  return True, []


def _trigger_reupload_of_missing_feed_files(
    storage_client: storage.client.Client, missing_files: List[str]) -> None:
  """Helper function that uploads a CF trigger to reprocess feed files."""
  if not missing_files:
    return

  retrigger_load_bucket = storage_client.get_bucket(
      os.environ.get('RETRIGGER_BUCKET'))
  retrigger_load_bucket.blob('REPROCESS_TRIGGER_FILE').upload_from_string(
      '\n'.join(missing_files))

  print(f'{missing_files} files were missing, so triggering a retry for them.')


def _set_table_expiration_date(bigquery_client: bigquery.client.Client,
                               table_id: str, duration_ms: int) -> None:
  """Sets the provided table's expiration to the provided duration.

  Args:
      bigquery_client: The BigQuery python client instance.
      table_id: A fully-qualified string reference to a BigQuery table in the
        format 'your-project.your_dataset.your_table'
      duration_ms: The number of milliseconds in the future when the table
        should expire.
  """
  target_table = bigquery_client.get_table(table_id)
  expiration_date = _get_current_time_in_utc() + datetime.timedelta(
      milliseconds=duration_ms)
  target_table.expires = expiration_date
  bigquery_client.update_table(target_table, ['expires'])


def _get_current_time_in_utc() -> datetime.datetime:
  """Helper function that wraps retrieving the current date and time in UTC."""
  return datetime.datetime.now(pytz.utc)
