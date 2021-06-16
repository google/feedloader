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

from google.cloud import bigquery
from google.cloud import exceptions
from google.cloud import storage

import iso8601
import pytz

# Set the default expiration of this Cloud Function to 9 minutes.
_EVENT_MAX_AGE_SECONDS = 540
_ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000


def import_storage_file_into_big_query(
    event: Dict[str, Any], context: 'google.cloud.functions.Context') -> None:
  """Background Cloud Function triggered by GCS bucket upload.

     This function converts the uploaded GCS file data into a BigQuery table.

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

  if _function_execution_exceeded_max_allowed_duration(context):
    return

  _log_uploaded_file_metadata(context, event)
  update_bucket_name = os.environ.get('UPDATE_BUCKET')

  if not update_bucket_name:
    logging.error(
        exceptions.NotFound('Update Bucket Environment Variable not found.'))
    return

  storage_client = storage.Client()

  try:
    eof_bucket = storage_client.get_bucket(update_bucket_name)
  except exceptions.NotFound:
    logging.error(
        exceptions.NotFound(f'Bucket {update_bucket_name} could not be found.'))
    return

  update_eof = eof_bucket.get_blob('EOF')

  # The EOF file may be uploaded by the bq-stage-changes CF if processing is
  # currently ongoing, so prevent this CF from continuing if it exists.
  if update_eof is not None:
    logging.error(
        RuntimeError((f'An EOF file was found in bucket: {update_bucket_name}, '
                      'indicating Feedloader is currently processing '
                      'a set of feeds into Content API. Please wait or '
                      'force remove the EOF file from the bucket.')))
    return

  # Cloud Functions executes before the file is visible in GCS, so check first.
  if _file_to_import_exists(storage_client, event['bucket'], event['name']):
    _perform_big_query_load(event['bucket'], event['name'])
  else:
    # Need to wait until the file is found, so raise to trigger an auto-retry.
    raise RuntimeError((
        f"GCS File {event['bucket']}/{event['name']} not detected in GCS yet. '"
        f'Retrying...'))


def _file_to_import_exists(storage_client: storage.client.Client,
                           bucket_name: str, file_name: str) -> bool:
  """Helper function that returns whether the given GCS file exists or not."""

  storage_bucket = storage_client.bucket(bucket_name)
  return storage.Blob(
      bucket=storage_bucket, name=file_name).exists(storage_client)


def _function_execution_exceeded_max_allowed_duration(
    context: 'google.cloud.functions.Context') -> bool:
  """Helper function that checks if the CF ran over the maximum allowed."""

  current_time = _get_current_time_in_utc()
  function_start_time = iso8601.parse_date(context.timestamp)
  event_age_seconds = (current_time - function_start_time).total_seconds()

  if event_age_seconds > _EVENT_MAX_AGE_SECONDS:
    logging.error(
        RuntimeError(
            f'Dropping event {context.event_id} with age {event_age_seconds} ms'
        ))
    return True
  return False


def _log_uploaded_file_metadata(context: 'google.cloud.functions.Context',
                                event: Dict[str, Any]) -> None:
  """Logs the Cloud Function event and file info to Stackdriver."""

  print(f'Event ID: {context.event_id}')
  print(f"Uploaded Filename: {event['bucket']}/{event['name']}")


def _get_current_time_in_utc() -> datetime.datetime:
  """Helper function that wraps retrieving the current date and time in UTC."""

  return datetime.datetime.now(pytz.utc)


def _perform_big_query_load(bucket_name: str, file_name: str) -> None:
  """Helper function that handles the loading of the GCS file data into BQ."""

  big_query_client = bigquery.Client()

  big_query_job_config = bigquery.LoadJobConfig(
      allow_jagged_rows=True,
      autodetect=True,
      encoding='UTF-8',
      field_delimiter='\t',
      quote_character='',
      skip_leading_rows=1,
      source_format=bigquery.SourceFormat.CSV,
      time_partitioning=bigquery.table.TimePartitioning(
          type_='DAY', expiration_ms=_ITEMS_TABLE_EXPIRATION_DURATION_MS),
  )

  gcs_uri = f'gs://{bucket_name}/{file_name}'
  feed_table_path = f"{os.environ.get('BQ_DATASET')}.items"

  big_query_load_job = big_query_client.load_table_from_uri(
      gcs_uri, feed_table_path, job_config=big_query_job_config)

  big_query_load_job.result()  # Waits for the job to complete.

  destination_table = big_query_client.get_table(feed_table_path)
  print('Loaded {} rows to table {}'.format(destination_table.num_rows,
                                            feed_table_path))
