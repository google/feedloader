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

"""Client module that handles connection with BigQuery."""

import logging
import string

from google import cloud
from google.cloud import bigquery


def generate_query_string(filepath: str, project_id: str) -> str:
  """Generates string format of a query.

  Args:
    filepath: The path to the file containing the input query.
    project_id: GCP project id.

  Returns:
    The query with the GCP project ID.

  Raises:
    IOError: an error occurs when a file which filepath refers to does not
    exist.
  """
  try:
    with open(filepath) as query_file:
      # Remove comment lines including license header.
      query_string = ''.join(
          line for line in query_file.readlines() if not line.startswith('#'))
      query_template = string.Template(query_string)
      return query_template.substitute(project_id=project_id)
  except IOError as io_error:
    raise IOError(
        'Query file does not exist: {}'.format(filepath)) from io_error


class BigQueryClient(object):
  """Client to bundle BigQuery manipulation."""

  def __init__(self, client: bigquery.Client,
               dataset_reference: bigquery.DatasetReference,
               table_reference: bigquery.TableReference) -> None:
    """Inits BigQueryClient.

    Args:
      client: bigquery.Client object.
      dataset_reference: bigquery.DatasetReference object.
      table_reference: bigquery.TableReference object.
    """
    self._bigquery_client = client
    self._dataset_reference = dataset_reference
    self._table_reference = table_reference

  @classmethod
  def from_service_account_json(cls, service_account_path: str, dataset_id: str,
                                table_id: str) -> 'BigQueryClient':
    """Factory method to retrieve JSON credentials while creating client.

    Args:
      service_account_path: Path to service account configuration file.
      dataset_id: BigQuery dataset id to manipulate.
      table_id: BigQuery table id to manipulate.

    Returns:
      The client created with the retrieved JSON credentials.
    """
    client = bigquery.Client.from_service_account_json(service_account_path)
    dataset_reference = client.dataset(dataset_id)
    table_reference = dataset_reference.table(table_id)
    return cls(
        client=client,
        dataset_reference=dataset_reference,
        table_reference=table_reference)

  def initialize_dataset_and_table(self, query: str) -> None:
    """Create necessary dataset and table.

    Args:
      query: SQL to create a table used to process items.
    """
    # Create the dataset if it does not exist yet.
    if not self._dataset_exists(self._dataset_reference):
      try:
        self._bigquery_client.create_dataset(self._dataset_reference)
        logging.info('Dataset "%s" successfully created.',
                     self._dataset_reference.dataset_id)
      except cloud.exceptions.Conflict:
        logging.info('Dataset "%s" already exists, not creating',
                     self._dataset_reference.dataset_id)

    # Create the table if it does not exist yet.
    if not self._table_exists(self._table_reference):
      job_config = bigquery.QueryJobConfig()
      job_config.destination = self._table_reference
      job_config.write_disposition = 'WRITE_EMPTY'

      try:
        query_job = self._bigquery_client.query(query, job_config=job_config)
        query_job.result()
        logging.info('Table "%s" successfully created.',
                     self._table_reference.table_id)
      except cloud.exceptions.Conflict:
        logging.info('Table "%s" already exists, not creating',
                     self._table_reference.table_id)

  def _dataset_exists(self,
                      dataset_reference: bigquery.DatasetReference) -> bool:
    """Checks if the dataset exists or not."""
    try:
      self._bigquery_client.get_dataset(dataset_reference)
      return True
    except cloud.exceptions.NotFound:
      return False

  def _table_exists(self, table_reference: bigquery.TableReference) -> bool:
    """Checks if the table exists or not."""
    try:
      self._bigquery_client.get_table(table_reference)
      return True
    except cloud.exceptions.NotFound:
      return False

  def delete_table(self) -> None:
    """Deletes the table."""
    self._bigquery_client.delete_table(self._table_reference)
    logging.info('Table "%s" has successfully been deleted',
                 self._table_reference.table_id)
