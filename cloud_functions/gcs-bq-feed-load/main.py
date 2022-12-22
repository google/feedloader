# coding=utf-8
# Copyright 2022 Google LLC.
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
import json
import logging
import os
from typing import Any, Collection, Dict

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage

import iso8601
import pytz
import schema

_ARCHIVE_FOLDER_PREFIX = 'archive/'
_CONFIG_FILENAME = 'config.json'
_EVENT_MAX_AGE_SECONDS = 540  # Default expiration of this CF is 9 minutes.
_ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000
_TABLE_PARTITION_GRANULARITY = 'DAY'


def import_storage_file_into_big_query(
    event: Dict[str, Any], context: 'google.cloud.functions.Context') -> None:
  """Cloud Function ("CF") triggered by a Cloud Storage ("GCS") bucket upload.

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
      None. The output is written to Cloud logging.
  """

  # Do not run this function if the file is meant for archival.
  if _ARCHIVE_FOLDER_PREFIX in event['name']:
    return

  if _function_execution_exceeded_max_allowed_duration(context):
    return

  _log_uploaded_file_metadata(context, event)
  update_bucket_name = os.environ.get('UPDATE_BUCKET')

  if not update_bucket_name:
    logging.error(
        exceptions.NotFound('Update Bucket Environment Variable not found.'))
    return

  update_bucket_name = update_bucket_name.replace('gs://', '')

  storage_client = storage.Client()

  try:
    eof_bucket = storage_client.get_bucket(update_bucket_name)
  except exceptions.NotFound:
    logging.error(
        exceptions.NotFound(f'Bucket {update_bucket_name} could not be found.'))
    return

  schema_config_contents = open(_CONFIG_FILENAME).read()
  schema_config = json.loads(schema_config_contents)
  if not _schema_config_valid(schema_config):
    logging.error(
        exceptions.BadRequest(f'Schema is invalid: {schema_config_contents} .'))
    return
  items_table_bq_schema = _parse_schema_config(schema_config)

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

  # This CF might execute before the file is visible in GCS, so check first.
  if _file_to_import_exists(storage_client, event['bucket'], event['name']):
    _perform_bigquery_load(event['bucket'], event['name'],
                           items_table_bq_schema)
  else:
    # Need to wait until the file is found, so raise to trigger an auto-retry.
    raise RuntimeError(
        (f"GCS File {event['bucket']}/{event['name']} not detected in GCS yet. "
         f'Retrying...'))

  _save_imported_filename_to_gcs(storage_client, event)


def _file_to_import_exists(storage_client: storage.client.Client,
                           bucket_name: str, filename: str) -> bool:
  """Helper function that returns whether the given GCS file exists or not."""

  storage_bucket = storage_client.get_bucket(bucket_name)
  return storage.Blob(
      bucket=storage_bucket, name=filename).exists(storage_client)


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


def _perform_bigquery_load(
    bucket_name: str, filename: str,
    items_table_bq_schema: Collection[bigquery.SchemaField]) -> None:
  """Helper function that handles the loading of the GCS file data into BQ.

  Args:
      bucket_name: The name of the Cloud Storage bucket to find the file in.
      filename: The name of the file in the bucket to convert to a BQ table.
      items_table_bq_schema: The BigQuery schema as defined in
        https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html#google.cloud.bigquery.job.LoadJobConfig.schema

  Raises:
    GoogleAPICallError: The BigQuery job failed to finish.
    TimeoutError: The job did not complete within the maximum allowed time.

  Returns:
      None. The output is written to Cloud logging.
  """
  bigquery_client = bigquery.Client()

  bigquery_job_config = bigquery.LoadJobConfig(
      allow_jagged_rows=True,
      encoding='UTF-8',
      field_delimiter='\t',
      quote_character='',
      schema=items_table_bq_schema,
      skip_leading_rows=1,
      source_format=bigquery.SourceFormat.CSV,
      time_partitioning=bigquery.table.TimePartitioning(
          type_=_TABLE_PARTITION_GRANULARITY,
          expiration_ms=_ITEMS_TABLE_EXPIRATION_DURATION_MS),
      write_disposition='WRITE_APPEND',
  )

  gcs_uri = f'gs://{bucket_name}/{filename}'
  feed_table_path = f"{os.environ.get('BQ_DATASET')}.items"

  bigquery_load_job = bigquery_client.load_table_from_uri(
      gcs_uri, feed_table_path, job_config=bigquery_job_config)

  try:
    bigquery_load_job.result()  # Waits for the job to complete.
  except (exceptions.GoogleAPICallError, TimeoutError) as error:
    logging.error(
        RuntimeError(
            f'BigQuery load job failed. Job ID: {bigquery_load_job.job_id}. '
            f'Error details: {error}'))

  destination_table = bigquery_client.get_table(feed_table_path)
  print('Loaded {} rows to table {}'.format(destination_table.num_rows,
                                            feed_table_path))


def _save_imported_filename_to_gcs(storage_client: storage.client.Client,
                                   event: Dict[str, Any]) -> None:
  """Helper function that records the imported file's name to a GCS bucket."""
  print('Starting insert of import history record...')

  completed_files_bucket_name = os.environ.get(
      'COMPLETED_FILES_BUCKET').replace('gs://', '')
  completed_files_bucket = storage_client.get_bucket(
      completed_files_bucket_name)

  completed_files_bucket.blob(event['name']).upload_from_string('')

  print(f"Imported filename: {event['name']} was saved into GCS bucket: "
        f'{completed_files_bucket_name} to confirm the upload succeeded.')


def _schema_config_valid(schema_config: Dict[str, Any]) -> bool:
  """Helper method that returns True if the config is in the correct format."""
  if not isinstance(schema_config.get('mapping'), list):
    return False

  items_table_schema = schema.Schema([{
      'csvHeader': str,
      'bqColumn': str,
      'columnType': str
  }])

  return items_table_schema.is_valid(schema_config.get('mapping'))


def _parse_schema_config(
    schema_config: Dict[str, Any]) -> Collection[bigquery.SchemaField]:
  """Transforms the items table schema config file into a BQ-loadable object."""
  bq_schema = [
      bigquery.SchemaField(column.get('bqColumn'), column.get('columnType'))
      for column in schema_config.get('mapping')
  ]
  return bq_schema
