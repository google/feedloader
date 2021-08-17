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
import enum
import json
import logging
import multiprocessing
import os
from typing import Any, Dict, List, Tuple

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import storage

import pytz

import queries

_BUCKET_DELIMITER = '/'
_COMPLETED_FILES_BUCKET = ''
_DELETES_TABLE_NAME = 'items_to_delete'
_EXPIRATION_TABLE_NAME = 'items_to_prevent_expiring'
_ITEMS_TABLE_EXPIRATION_DURATION = 43200000  # 12 hours.
_ITEMS_TABLE_NAME = 'items'
_ITEMS_TO_DELETE_TABLE_NAME = 'items_to_delete'
_ITEMS_TO_PREVENT_EXPIRING_TABLE_NAME = 'items_to_prevent_expiring'
_ITEMS_TO_UPSERT_TABLE_NAME = 'items_to_upsert'
_STREAMING_ITEMS_TABLE_NAME = 'streaming_items'
_LOCK_FILE_NAME = 'EOF.lock'
_MERCHANT_ID_COLUMN = 'google_merchant_id'
_STREAMING_ITEMS_TABLE_NAME = 'streaming_items'
_UPSERTS_TABLE_NAME = 'items_to_upsert'
_WRITE_DISPOSITION = enum.Enum('WRITE_DISPOSITION',
                               'WRITE_TRUNCATE WRITE_APPEND WRITE_EMPTY')


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
      The RuntimeError is not raised explicitly in this function but is default
      behavior for any Cloud Function.

  Returns:
      None; the output is written to Cloud logging.
  """
  del context

  (bq_dataset, expiration_threshold, feed_bucket, gcp_project, lock_bucket,
   retrigger_bucket, timezone_utc_offset) = _load_environment_variables()

  fully_qualified_items_table_name = (
      f'{gcp_project}.{bq_dataset}.{_ITEMS_TABLE_NAME}')
  fully_qualified_items_to_delete_table_name = (
      f'{gcp_project}.{bq_dataset}.{_ITEMS_TO_DELETE_TABLE_NAME}')
  fully_qualified_items_to_prevent_expiring_table_name = (
      f'{gcp_project}.{bq_dataset}.{_ITEMS_TO_PREVENT_EXPIRING_TABLE_NAME}')
  fully_qualified_items_to_upsert_table_name = (
      f'{gcp_project}.{bq_dataset}.{_ITEMS_TO_UPSERT_TABLE_NAME}')
  fully_qualified_streaming_items_table_name = (
      f'{gcp_project}.{bq_dataset}.{_STREAMING_ITEMS_TABLE_NAME}')

  if _eof_is_invalid(event):
    return

  bigquery_client = bigquery.Client()
  storage_client = storage.Client()

  # If the CF was not triggered by a retry, then handle the locking routine.
  if event['name'] != 'EOF.retry':
    if _lock_exists(storage_client, lock_bucket):
      logging.error(
          exceptions.AlreadyExists(
              ('An EOF.lock file was found, indicating that this CF is still '
               'running. Exiting Function...')))
      return
    # Lock the EOF file at this point to prevent concurrent runs.
    _lock_eof(storage_client, event['bucket'], event['name'], lock_bucket)

  print('Empty EOF file detected. Checking files were imported successfully...')

  import_successful, missing_files = (
      _ensure_all_files_were_imported(storage_client, bigquery_client,
                                      feed_bucket, lock_bucket,
                                      fully_qualified_items_table_name))

  if not import_successful:
    _trigger_reupload_of_missing_feed_files(storage_client, missing_files,
                                            retrigger_bucket)
    return
  else:
    print('File import check successful. Proceeding to cleanup completed '
          'filenames from Cloud Storage...')
    try:
      cleanup_completed_files_successful = (
          _cleanup_completed_filenames(storage_client))
      if not cleanup_completed_files_successful:
        logging.error(
            RuntimeError(
                'Cleanup completed filenames failed. Cleaning up and exiting...'
            ))
        return
    except Exception as cleanup_completed_files_error:  
      # refex: disable=pytotw.037
      logging.error(
          RuntimeError(
              'Cleanup completed filenames failed. Cleaning up and exiting...'),
          cleanup_completed_files_error)
      _clean_up(storage_client, bigquery_client, lock_bucket,
                fully_qualified_items_table_name)
      return

  print(
      'Completed files cleanup finished. Setting expiration on items table...')

  _set_table_expiration_date(bigquery_client, fully_qualified_items_table_name,
                             _ITEMS_TABLE_EXPIRATION_DURATION)

  print('Expiration set on items table. Proceeding to archive feed files...')

  try:
    _archive_folder(storage_client, feed_bucket)
  except Exception as archive_error:  
    # Stackdriver does not log errors to GCP unless using .error(Exception).
    # refex: disable=pytotw.037
    logging.error(
        RuntimeError(
            'One or more errors occurred in archiving. Cleaning up...'),
        archive_error)
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  print('All the feeds were loaded and archiving finished. Checking all the '
        'required Big Query tables exist...')

  table_list = [
      fully_qualified_items_table_name,
      fully_qualified_items_to_delete_table_name,
      fully_qualified_items_to_prevent_expiring_table_name,
      fully_qualified_items_to_upsert_table_name,
      fully_qualified_streaming_items_table_name
  ]
  if not all(_table_exists(bigquery_client, table) for table in table_list):
    logging.error(
        RuntimeError(
            'One or more necessary tables are missing. Make sure they exist. '
            'Cleaning up...'))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  print('Confirmed required Big Query tables existence. Starting '
        'calculate_product_changes...')

  try:
    query_hash_statements, merchant_id_column = _parse_bigquery_config()
  except Exception as parse_bigquery_config_error:  
    # refex: disable=pytotw.037
    logging.error(
        RuntimeError('Parsing the config file failed.'),
        parse_bigquery_config_error)
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  copy_item_batch_query = queries.COPY_ITEM_BATCH_QUERY.replace(
      '{{COLUMNS_TO_HASH}}', query_hash_statements).replace(
          '{{MC_COLUMN}}', merchant_id_column).replace('{{BQ_DATASET}}',
                                                       bq_dataset)
  try:
    # Copy items table in order to add hashes and timestamps to it.
    _run_materialize_job(bigquery_client, bq_dataset,
                         _STREAMING_ITEMS_TABLE_NAME, gcp_project,
                         copy_item_batch_query,
                         _WRITE_DISPOSITION.WRITE_APPEND.name)
  except Exception as copy_items_batch_error:  
    logging.error(str(copy_items_batch_error))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  calculate_deletions_query = (
      queries.CALCULATE_ITEMS_FOR_DELETION_QUERY.replace(
          '{{BQ_DATASET}}', bq_dataset))

  try:
    # Find out how many items need to be deleted, if any.
    _run_materialize_job(bigquery_client, bq_dataset, _DELETES_TABLE_NAME,
                         gcp_project, calculate_deletions_query,
                         _WRITE_DISPOSITION.WRITE_TRUNCATE.name)
  except Exception as deletions_calculation_error:  
    logging.error(str(deletions_calculation_error))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  calculate_updates_query = queries.CALCULATE_ITEMS_FOR_UPDATE_QUERY.replace(
      '{{COLUMNS_TO_HASH}}',
      query_hash_statements).replace('{{BQ_DATASET}}', bq_dataset)

  try:
    # Start the "upserts" calculation. This is done with two separate queries,
    # one for updates, one for inserts.
    _run_materialize_job(bigquery_client, bq_dataset, _UPSERTS_TABLE_NAME,
                         gcp_project, calculate_updates_query,
                         _WRITE_DISPOSITION.WRITE_TRUNCATE.name)
  except Exception as updates_calculation_error:  
    logging.error(str(updates_calculation_error))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  calculate_inserts_query = queries.CALCULATE_ITEMS_FOR_INSERTION_QUERY.replace(
      '{{BQ_DATASET}}', bq_dataset)

  try:
    # Newly inserted items cannot rely on hashes used by calculate_updates_query
    # to detect them, so append these results to the upserts table, too.
    _run_materialize_job(bigquery_client, bq_dataset, _UPSERTS_TABLE_NAME,
                         gcp_project, calculate_inserts_query,
                         _WRITE_DISPOSITION.WRITE_APPEND.name)
  except Exception as inserts_calculation_error:  
    logging.error(str(inserts_calculation_error))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return

  calculate_expirations_query = queries.GET_EXPIRING_ITEMS_QUERY.replace(
      '{{BQ_DATASET}}', bq_dataset).replace('{{TIMEZONE_UTC_OFFSET}}',
                                            timezone_utc_offset).replace(
                                                '{{EXPIRATION_THRESHOLD}}',
                                                expiration_threshold)

  try:
    # Populate the items to prevent expiring table with items that have not been
    # touched in EXPIRATION_THRESHOLD days.
    _run_materialize_job(bigquery_client, bq_dataset, _EXPIRATION_TABLE_NAME,
                         gcp_project, calculate_expirations_query,
                         _WRITE_DISPOSITION.WRITE_TRUNCATE.name)
  except Exception as expirations_calculation_error:  
    logging.error(str(expirations_calculation_error))
    _clean_up(storage_client, bigquery_client, lock_bucket,
              fully_qualified_items_table_name)
    return


def _load_environment_variables() -> Tuple[str, str, str, str, str, str, str]:
  """Helper function that loads all environment variables."""
  bq_dataset = os.environ.get('BQ_DATASET')
  expiration_threshold = str(os.environ.get('EXPIRATION_THRESHOLD'))
  gcp_project = os.environ.get('GCP_PROJECT')
  retrigger_bucket = os.environ.get('RETRIGGER_BUCKET')
  timezone_utc_offset = os.environ.get('TIMEZONE_UTC_OFFSET')

  # Strip out the bucket prefixes in case the user set their env var with one.
  completed_files_bucket = os.environ.get('COMPLETED_FILES_BUCKET').replace(
      'gs://', '')
  feed_bucket = os.environ.get('FEED_BUCKET').replace('gs://', '')
  lock_bucket = os.environ.get('LOCK_BUCKET').replace('gs://', '')

  # Global, because multiprocessing lib only accepts a single-argument function.
  global _COMPLETED_FILES_BUCKET
  _COMPLETED_FILES_BUCKET = completed_files_bucket

  return (bq_dataset, expiration_threshold, feed_bucket, gcp_project,
          lock_bucket, retrigger_bucket, timezone_utc_offset)


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


def _lock_exists(storage_client: storage.client.Client,
                 lock_bucket: str) -> bool:
  """Helper method that returns True if EOF.lock exists, otherwise False."""
  eof_lock_bucket = storage_client.get_bucket(lock_bucket)
  return storage.Blob(
      bucket=eof_lock_bucket, name=_LOCK_FILE_NAME).exists(storage_client)


def _lock_eof(storage_client: storage.client.Client, eof_bucket_name: str,
              eof_filename: str, lock_bucket: str) -> None:
  """Helper function that sets the EOF to a "locked" state."""
  eof_bucket = storage_client.get_bucket(eof_bucket_name)
  eof_blob = eof_bucket.get_blob(eof_filename)
  lock_destination = storage_client.get_bucket(lock_bucket)
  eof_bucket.copy_blob(eof_blob, lock_destination, new_name=_LOCK_FILE_NAME)
  eof_blob.delete()


def _table_exists(bigquery_client: bigquery.client.Client,
                  table_name: str) -> bool:
  """Checks if BigQuery table exists or not."""
  try:
    bigquery_client.get_table(table_name)
  except exceptions.NotFound:
    logging.error(
        exceptions.NotFound(
            f'Table {table_name} must exist before running the product'
            f' calculation function.'
        ))
    return False
  return True


def _ensure_all_files_were_imported(
    storage_client: storage.client.Client,
    bigquery_client: bigquery.client.Client, feed_bucket: str, lock_bucket: str,
    items_table_name: str) -> Tuple[bool, List[str]]:
  """Helper function that checks attempted feeds against expected filenames."""

  attempted_feed_files_iterator = storage_client.list_blobs(
      feed_bucket, delimiter=_BUCKET_DELIMITER)
  attempted_feed_files = list(attempted_feed_files_iterator)
  if not attempted_feed_files:
    logging.error(
        exceptions.NotFound(
            'Attempted feeds retrieval failed, or no files are in the bucket.'))
    _clean_up(storage_client, bigquery_client, lock_bucket, items_table_name)
    return False, []
  attempted_filenames = [feed.name for feed in attempted_feed_files]

  completed_feed_files_iterator = storage_client.list_blobs(
      _COMPLETED_FILES_BUCKET)
  completed_feed_files = list(completed_feed_files_iterator)
  if not completed_feed_files:
    logging.error(
        exceptions.NotFound(
            'Completed filenames retrieval failed, or no files in the bucket.'))
    _clean_up(storage_client, bigquery_client, lock_bucket, items_table_name)
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
    storage_client: storage.client.Client, missing_files: List[str],
    retrigger_bucket: str) -> None:
  """Helper function that uploads a CF trigger to reprocess feed files."""
  if not missing_files:
    return

  retrigger_load_bucket = storage_client.get_bucket(retrigger_bucket)
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


def _cleanup_completed_filenames(storage_client: storage.client.Client) -> bool:
  """Deletes all files from the completed bucket.

  Args:
    storage_client: The Cloud Storage python client instance.

  Returns:
    True if all deletions succeeded, otherwise False.
  """
  completed_file_blobs = storage_client.list_blobs(_COMPLETED_FILES_BUCKET)
  with multiprocessing.Pool() as pool:
    results = list(
        pool.imap_unordered(_delete_completed_file,
                            (blob.name for blob in completed_file_blobs)))
    return all(results)
  return False


def _delete_completed_file(filename: str) -> bool:
  """Deletes the specified file from the completed files bucket.

  Args:
    filename: The name of the blob file to delete.

  Returns:
    True if the file was deleted successfully, otherwise False.
  """
  storage_client = storage.Client()
  completed_files_bucket = storage_client.get_bucket(_COMPLETED_FILES_BUCKET)
  try:
    completed_files_bucket.delete_blob(filename)
  except exceptions.NotFound:
    logging.error(
        exceptions.NotFound(
            f'Failed to delete {filename} in {_COMPLETED_FILES_BUCKET}.'))
    return False
  return True


def _archive_folder(storage_client: storage.client.Client,
                    feed_bucket: str) -> None:
  """Renames current feeds to subfolder with timestamp for archival purposes."""
  feed_files = storage_client.list_blobs(
      feed_bucket, delimiter=_BUCKET_DELIMITER)
  feed_bucket = storage_client.get_bucket(feed_bucket)
  current_datetime = _get_current_time_in_utc().strftime('%Y_%m_%d_%H_%M_%p')
  for feed_file_to_archive in feed_files:
    archive_destination = f'archive/{current_datetime}/{feed_file_to_archive.name}'
    result = feed_bucket.rename_blob(feed_file_to_archive, archive_destination)
    if not result:
      raise exceptions.GoogleAPICallError(
          f'rename_blob failed for {feed_file_to_archive.name}')


def _clean_up(storage_client: storage.client.Client,
              bigquery_client: bigquery.client.Client,
              lock_bucket: str,
              items_table_name: str,
              clean_items_table=True) -> None:
  """Cleans up the state of the run (items table and EOF) upon error cases."""
  eof_lock_bucket = storage_client.get_bucket(lock_bucket)

  # get_blob returns None if it doesn't exist, so no need to call exists().
  eof_lock_file = eof_lock_bucket.get_blob(_LOCK_FILE_NAME)

  if eof_lock_file:
    eof_lock_file.delete()

  if clean_items_table:
    bigquery_client.delete_table(items_table_name, not_found_ok=True)


def _parse_bigquery_config() -> Tuple[str, str]:
  """Validates, parses, and generates SQL from the BigQuery schema config file."""
  schema_config_file = open('config.json',)
  schema_config = json.load(schema_config_file)
  config_exists = (schema_config and schema_config.get('mapping')) or False

  if not config_exists or not isinstance(schema_config['mapping'], list):
    logging.error(
        'Unable to map any columns from the schema config. Aborting...')
    raise exceptions.BadRequest('config.json could not be parsed.')

  query_hash_statements = ', '.join([
      f"IFNULL(CAST(Items.{mapping['bqColumn']} AS STRING), 'NULL')"
      for mapping in schema_config['mapping']
  ])

  return (query_hash_statements, f'{_MERCHANT_ID_COLUMN},' if
          (_MERCHANT_ID_COLUMN in query_hash_statements) else '')


def _run_materialize_job(bigquery_client: bigquery.client.Client,
                         bq_dataset: str, destination_table: str,
                         gcp_project: str, query: str,
                         write_disposition: str) -> None:
  """Helper function that runs a query with the specified query job settings."""
  print(f'Starting BigQuery job for {destination_table}...')
  big_query_job_config = bigquery.QueryJobConfig(
      destination=f'{gcp_project}.{bq_dataset}.{destination_table}',
      write_disposition=write_disposition)

  query_job = bigquery_client.query(
      query,
      job_config=big_query_job_config,
  )
  query_job.result()
  print(f'BigQuery job finished for {destination_table}.')
