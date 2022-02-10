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

"""Airflow operator to clean up the data to make the appliation ready for the next upload."""

import logging
from typing import Any, Mapping, Text

from airflow import exceptions
from airflow import models
from airflow.contrib.hooks import bigquery_hook
from airflow.contrib.hooks import gcs_hook
from google.cloud import exceptions as cloud_exceptions


class CleanUpOperator(models.BaseOperator):
  """Airflow operator to clean up the data."""

  def __init__(self, dataset_id: Text, table_id: Text, bucket_id: Text, *args,
               **kwargs):
    """Inits CleanUpOperator.

    Args:
      dataset_id: BigQuery dataset ID.
      table_id: BigQuery table ID.
      bucket_id: Cloud Storage bucket ID.
      *args: arguments to initialize the super class.
      **kwargs: keyword arguments to initialize the super class.
    """
    super(CleanUpOperator, self).__init__(*args, **kwargs)
    self._dataset_id = dataset_id
    self._table_id = table_id
    self._bucket_id = _retrieve_bucket_name(bucket_id)

  def execute(self, context: Mapping[Text, Any]) -> None:
    """Executes operator.

    Args:
      context: Airflow context that contains references to related objects to
        the task instance.

    Raises:
       airflow.AirflowException: Raised when the task failed to call BigQuery
        API.
    """
    logging.info('Starting cleanup routine...')
    try:
      bq_hook = bigquery_hook.BigQueryHook()
      bq_cursor = bq_hook.get_cursor()
      table_name = f'{self._dataset_id}.{self._table_id}'
      bq_cursor.run_table_delete(
          deletion_dataset_table=table_name, ignore_if_missing=True)
      logging.info('Successfully deleted table: %s', table_name)
    except Exception as bq_api_error:
      raise exceptions.AirflowException(
          'BigQuery API returned an error while deleting the items table.'
      ) from bq_api_error
    try:
      storage_hook = gcs_hook.GoogleCloudStorageHook()
      storage_hook.delete(bucket=self._bucket_id, object='EOF.lock')
      logging.info('Successfully deleted the EOF.lock file.')
    except cloud_exceptions.GoogleCloudError as gcs_api_error:
      raise exceptions.AirflowException(
          'Cloud Storage API returned an error while deleting EOF.lock.'
      ) from gcs_api_error
    logging.info('Clean up task finished!')


def _retrieve_bucket_name(bucket_url_or_name: str) -> str:
  """Returns bucket name retrieved from URL.

  Args:
    bucket_url_or_name: The name or url of the storage bucket.
  """
  return bucket_url_or_name.split('/')[-1]
