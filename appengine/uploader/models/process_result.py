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

# python3
"""Class to save the results of Content API calls."""

import dataclasses

from models import failure


@dataclasses.dataclass()
class ProcessResult(object):
  """Class to save the results of Content API calls."""

  successfully_processed_item_ids: []
  content_api_failures: []
  skipped_item_ids: []

  def add_successfully_processed_item_id(self, item_id: str) -> None:
    """Adds an item id to the list of successful items ids.

    Args:
      item_id: The item id to add.
    """
    self.successfully_processed_item_ids.append(item_id)

  def add_content_api_failure(self, fail: failure.Failure) -> None:
    """Adds a failure to the list of failures.

    Args:
      fail: The failure to add.
    """
    self.content_api_failures.append(fail)

  def add_skipped_item_id(self, item_id: str) -> None:
    """Adds an item id to the list of skipped items ids.

    Args:
      item_id: The item id to add.
    """
    self.skipped_item_ids.append(item_id)

  def get_success_count(self) -> int:
    """Returns the number of items that received a success response from Content API.

    Returns:
      The number of item ids that received a successful response from Content
      API.
    """
    return len(self.successfully_processed_item_ids)

  def get_failure_count(self) -> int:
    """Returns the number of items that received errors in the response from Content API.

    Returns:
      The number of item ids that received errors in the response from Content
      API.
    """
    return len(self.content_api_failures)

  def get_skipped_count(self) -> int:
    """Returns the number of items that are missing a merchant id when using a multi-merchant MCA.

    Returns:
      The number of items that are missing a merchant id when using a
      multi-merchant MCA.
    """
    return len(self.skipped_item_ids)

  def get_counts_str(self) -> str:
    """Returns the result counts as a formatted string.

    If the data fails to insert into BigQuery, this can be used to write
    the results to the logs instead.

    Returns:
      The success/failure/skipped counts as a formatted string.
    """
    return (f'Success: {self.get_success_count()},'
            f' Failure: {self.get_failure_count()}, Skipped: '
            f'{self.get_skipped_count()}.')

  def get_ids_str(self) -> str:
    """Returns the Content ID result for each ID as a formatted string.

    If the data fails to insert into BigQuery, this can be used to write the
    results to the logs instead.

    Returns:
      The list of successful/failed/skipped IDs as a formatted string.
    """
    success_items = ', '.join(self.successfully_processed_item_ids)
    failure_items = [
        f'ID: {failed.item_id}, Error: {failed.error_msg}, '
        for failed in self.content_api_failures
    ]
    skipped_items = ', '.join(self.skipped_item_ids)

    return (f'Success: {success_items}, Failure: '
            f'{failure_items}, Skipped: {skipped_items}.')
