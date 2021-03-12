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

"""Custom BigQuery Hooks."""

import http
import logging
from typing import Text

from airflow.contrib.hooks import bigquery_hook
from googleapiclient import errors


class BigQueryHook(bigquery_hook.BigQueryHook):
  """Custom BigQuery Hook."""

  def delete_table(self, dataset_id: Text, table_id: Text) -> None:
    """Deletes the specified BigQuery table.

    Args:
      dataset_id: BigQuery dataset ID that contains the table to delete.
      table_id: The ID of the table to delete.

    Raises:
      BigQueryDeleteTableError: Raised when table deletion failed for any reason
      other than not found.
    """
    table_path = f'{self.project_id}.{dataset_id}.{table_id}'
    service = self.get_service()
    try:
      service.tables().delete(
          projectId=self.project_id, datasetId=dataset_id,
          tableId=table_id).execute()
      logging.info('Table has been successfully deleted: %s', table_path)
    except errors.HttpError as http_error:
      if http_error.resp.status == http.HTTPStatus.NOT_FOUND:
        logging.info('Table does not exist: %s', table_path)
      else:
        raise BigQueryApiError(
            f'Failed to delete a BigQuery table: {table_path}') from http_error


class BigQueryApiError(Exception):
  """Raised when deleting BigQuery table failed."""
  pass
