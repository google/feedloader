# coding=utf-8
# Copyright 2023 Google LLC.
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

from typing import List

from google.cloud import bigquery


class BigQueryClient(object):
  """Client to bundle BigQuery manipulation."""

  def __init__(self, client: bigquery.Client,
               table_reference: bigquery.TableReference) -> None:
    """Initializes BigQueryClient.

    Args:
      client: bigquery.Client object.
      table_reference: bigquery.TableReference object.
    """
    self._client = client
    self._table_reference = table_reference

  @classmethod
  def from_service_account_json(cls, service_account_path: str, dataset_id: str,
                                table_id: str) -> 'BigQueryClient':
    """Factory to retrieve JSON credentials while creating client.

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
    return cls(client=client, table_reference=table_reference)

  def load_items(self, start_index: int, batch_size: int) -> List[bigquery.Row]:
    """Fetches updated item data from BigQuery.

    Args:
      start_index: Offset used to specify items that the batch job should
        handle.
      batch_size: Number of items to fetch from BigQuery.

    Returns:
      List of bigquery.Row object that that has data of
      updated items specified by the start_index and batch_size.
      Reference:
      https://github.com/GoogleCloudPlatform/google-cloud-python/blob/master/bigquery/google/cloud/bigquery/table.py
      
    """
    table = self._client.get_table(self._table_reference)
    row_iterator = self._client.list_rows(
        table, start_index=start_index, max_results=batch_size)
    return list(row_iterator)
