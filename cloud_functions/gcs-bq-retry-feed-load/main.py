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
import json
import logging
import os
from typing import Any, Collection, Dict, List

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage

import iso8601
import pytz
import schema

_CONFIG_FILENAME = 'config.json'
_EOF_RETRY_FILENAME = 'EOF.retry'
_EVENT_MAX_AGE_SECONDS = 540  # Default expiration of this CF is 9 minutes.
_ITEMS_TABLE_EXPIRATION_DURATION_MS = 43200000  # 12 hours.
_ITEMS_TABLE_NAME = 'items'


def reprocess_feed_file(event: Dict[str, Any],
                        context: 'google.cloud.functions.Context') -> None:
  """Cloud Function that re-uploads the provided file into BigQuery.

  Args:
      event:  The dictionary with data specific to this type of event. The
        'data' field contains a description of the event in the Cloud Storage
        'object' format described here:
        https://cloud.google.com/storage/docs/json_api/v1/objects#resource
      context: Metadata of triggering event.

  Raises:
    RuntimeError: A dependency was not found, requiring this CF to exit.
      The RuntimeError is not raised explicitly in this function but is
      default behavior for any Cloud Function.

  Returns:
      None. The output is written to Cloud logging.
  """
  # Prevent long-running retries.
  if _function_execution_exceeded_max_allowed_duration(context):
    return

  schema_config_file = open(_CONFIG_FILENAME,)
  schema_config = json.load(schema_config_file)
  if not _schema_config_valid(schema_config):
    logging.error(
        exceptions.BadRequest(f'Schema is invalid: {schema_config} .'))
    return
  items_table_bq_schema = _parse_schema_config(schema_config)

  print('Starting reprocess_feed_file Cloud Function...')
  storage_client = storage.Client()
  retrigger_bucket = storage_client.get_bucket(
      os.environ.get('RETRIGGER_BUCKET'))
  missing_files_blob = retrigger_bucket.get_blob(event['name'])
  missing_files_bytes = missing_files_blob.download_as_string()
  missing_files = []

  if missing_files_bytes:
    missing_files = missing_files_bytes.decode('utf-8').splitlines()

  if not missing_files:
    print('No more files to reprocess. Uploading a retry EOF...')
    _retrigger_calculation_function(storage_client)
  else:
    file_to_reprocess = missing_files.pop(0)
    _reprocess_file(storage_client, file_to_reprocess, items_table_bq_schema)
    _reupload_file_list(storage_client, missing_files, event['name'])


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


def _get_current_time_in_utc() -> datetime.datetime:
  """Helper function that wraps retrieving the current date and time in UTC."""
  return datetime.datetime.now(pytz.utc)


def _reprocess_file(
    storage_client: storage.client.Client, filename: str,
    items_table_bq_schema: Collection[bigquery.SchemaField]) -> None:
  """Reloads the specified filename from Cloud Storage into BigQuery.

  Args:
      storage_client: The GCS object instance.
      filename: The name of the file to reprocess.
      items_table_bq_schema: A parsed list of BigQuery schema columns.
  """
  print(f'Attempting reprocess of file {filename} into BigQuery...')
  _perform_bigquery_load(
      os.environ.get('FEED_BUCKET'), filename, items_table_bq_schema)

  print(f'File:{filename} was re-loaded into BigQuery successfully. '
        'Starting insert of import history record...')
  _save_imported_filename_to_gcs(storage_client, filename)


def _reupload_file_list(storage_client: storage.client.Client,
                        file_list: List[str], filename: str) -> None:
  """Reuploads list of missing files to GCS, triggers calculate_product_changes.

  Args:
      storage_client: The GCS object instance.
      file_list: A List containing each filename representing missing files.
      filename: The filename to re-upload to the bucket.
  """
  file_list_str_rejoined = '\n'.join(file_list)
  retrigger_bucket_name = os.environ.get('RETRIGGER_BUCKET').replace(
      'gs://', '')
  retrigger_bucket = storage_client.get_bucket(retrigger_bucket_name)
  retrigger_bucket.blob(filename).upload_from_string(file_list_str_rejoined)


def _retrigger_calculation_function(storage_client: storage.client.Client()
                                   ) -> None:
  """Uploads an empty EOF.retry file to re-trigger the calculate_product_changes CF."""
  update_bucket_name = os.environ.get('UPDATE_BUCKET').replace('gs://', '')
  update_bucket = storage_client.get_bucket(update_bucket_name)
  update_bucket.blob(_EOF_RETRY_FILENAME).upload_from_string('')


def _schema_config_valid(schema_config: Dict[str, Any]) -> bool:
  """Helper method that returns True if the config is in the correct format."""
  if not isinstance(schema_config.get('mapping'), list):
    return False

  items_table_schema = schema.Schema([{
      'csvHeader': str,
      'bqColumn': str,
      'columnType': str,
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
          type_='DAY', expiration_ms=_ITEMS_TABLE_EXPIRATION_DURATION_MS),
      write_disposition='WRITE_APPEND',
  )

  gcs_uri = f'gs://{bucket_name}/{filename}'
  feed_table_path = f"{os.environ.get('BQ_DATASET')}.{_ITEMS_TABLE_NAME}"

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
                                   filename: str) -> None:
  """Helper function that records the imported file's name to a GCS bucket."""
  print('Starting insert of import history record...')

  completed_files_bucket_name = os.environ.get(
      'COMPLETED_FILES_BUCKET').replace('gs://', '')
  completed_files_bucket = storage_client.get_bucket(
      completed_files_bucket_name)

  completed_files_bucket.blob(filename).upload_from_string('')

  print(f'Imported filename: {filename} was saved into GCS bucket: '
        f'{completed_files_bucket_name} to confirm the upload succeeded.')
