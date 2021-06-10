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
import datetime
import logging
import os
from typing import Any, Dict

from google.cloud import exceptions
from google.cloud import storage

import iso8601
import pytz

# Set the default expiration of this Cloud Function to 9 minutes
_EVENT_MAX_AGE_SECONDS = 540


def import_storage_file_into_big_query(
    event: Dict[str, Any], context: 'google.cloud.functions.Context') -> None:
  """Background Cloud Function triggered by GCS bucket upload.

     This function converts the uploaded GCS file data into a BigQuery table.

  Args:
      event:  The dictionary with data specific to this type of event. The
        `data` field contains a description of the event in
                     the Cloud Storage `object` format described here:
                     https://cloud.google.com/storage/docs/json_api/v1/objects#resource
      context: Metadata of triggering event.

  Returns:
      None; the output is written to Stackdriver Logging.
  """

  if _function_execution_exceeded_max_allowed_duration(context):
    return

  _log_uploaded_file_metadata(context, event)

  storage_client = storage.Client()

  update_bucket_name = os.environ.get('UPDATE_BUCKET')

  if not update_bucket_name:
    logging.error('Update Bucket Environment Variable was not found.')
    return

  try:
    eof_bucket = storage_client.get_bucket(update_bucket_name)
  except exceptions.NotFound:
    logging.error('Bucket %s could not be found.', update_bucket_name)
    return

  update_eof = eof_bucket.get_blob('EOF')
  if update_eof is not None:
    logging.error('EOF file was found in bucket: %s', update_bucket_name)
    return


def _function_execution_exceeded_max_allowed_duration(
    context: 'google.cloud.functions.Context') -> bool:
  """Helper function that checks if the CF ran over the maximum allowed."""

  current_time = _get_current_time_in_utc()
  function_start_time = iso8601.parse_date(context.timestamp)
  event_age_seconds = (current_time - function_start_time).total_seconds()

  if event_age_seconds > _EVENT_MAX_AGE_SECONDS:
    logging.error('Dropping event %s with age %d ms.', context.eventId,
                  event_age_seconds)
    return True
  return False


def _log_uploaded_file_metadata(context: 'google.cloud.functions.Context',
                                event: Dict[str, Any]) -> None:
  """Logs the Cloud Function event and file info to Stackdriver."""

  logging.info('Event ID: %s', context.event_id)
  logging.info('Event type: %s', context.event_type)
  logging.info('Bucket: %s', event['bucket'])
  logging.info('Uploaded Filename: %s', event['name'])
  logging.info('Metageneration: %s', event['metageneration'])
  logging.info('Created: %s', event['timeCreated'])
  logging.info('Updated: %s', event['updated'])


def _get_current_time_in_utc() -> datetime.datetime:
  """Helper function that wraps retrieving the current date and time in UTC."""

  return datetime.datetime.now(pytz.utc)
