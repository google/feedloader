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

"""Cloud Function that triggers on GCS bucket upload to import data into BQ."""
import logging
import os

from typing import Any, Dict

from google.api_core import exceptions
from google.cloud import storage

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

  storage_client = storage.Client()

  if _eof_is_invalid(event):
    return

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

  print('Empty EOF file detected. Starting diff calculation Cloud Function...')


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
