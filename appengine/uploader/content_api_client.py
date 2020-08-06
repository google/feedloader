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
"""Adds products to the specified GMC account via the Google Shopping API.

  Typical usage example:

  client = MerchantCenterClient()
  client.process_items(item_list)
"""

import logging
import socket
from typing import Any, Dict, List, Tuple

from googleapiclient import discovery
from googleapiclient import errors

import constants
import content_api_helper
from models import failure

_RETRY_STATUS_CODE = (
    constants.ShoppingApiErrorCodes.AUTHENTICATION_ERROR.value,
    constants.ShoppingApiErrorCodes.ACCESS_ERROR.value,
    constants.ShoppingApiErrorCodes.QUOTA_LIMITS_REACHED.value,
    constants.ShoppingApiErrorCodes.INTERNAL_SERVER_ERROR.value,
    constants.ShoppingApiErrorCodes.SOCKET_TIMEOUT.value,
)


class ContentApiClient(object):
  """Client to bundle Google Merchant Center manipulation."""

  def __init__(self, content_api_service: discovery.Resource = None):
    if content_api_service:
      self._service = content_api_service
    else:
      self._service = content_api_helper.initialize_api(
          constants.CONFIG_DIRECTORY, constants.MC_SERVICE_ACCOUNT_FILE)

  def process_items(
      self, batch: constants.Batch, batch_number: int,
      batch_id_to_item_id: constants.BatchIdToItemId,
      method: constants.Method) -> Tuple[List[str], List[failure.Failure]]:
    """Processes a list of items via a single batch to the API.

    Args:
      batch: The batch to be sent to the API.
      batch_number: Identifier for this batch.
      batch_id_to_item_id: Mapping from batch_id to item_id.
      method: Method being sent to the API.

    Returns:
      An instance of ContentApiResult.
    """
    try:
      number_of_items = len(batch.get('entries', []))
      logging.info('Batch #%d: Received %d items to submit via API',
                   batch_number, number_of_items)
      response = self._submit_batch(batch)
      successful_item_ids, item_failures = _handle_response(
          response, batch_number, batch_id_to_item_id)
      logging.info(
          'Batch #%d: custombatch %s API successfully submitted %d of %d items',
          batch_number, method.value, len(successful_item_ids), number_of_items)
    except errors.HttpError as http_error:
      logging.exception(
          'Batch #%d: Unhandled error occurred during batch call to API: %s',
          batch_number, http_error)
      raise
    except socket.timeout as timeout_error:
      logging.exception(
          'Batch #%d: Socket timeout error occurred: %s. Check Merchant Center to confirm that the API call succeeded.',
          batch_number, timeout_error)
      raise
    return successful_item_ids, item_failures

  def _submit_batch(self, batch: constants.Batch) -> Dict[str, Any]:
    """Takes a batch object (JSON) and submits to GMC via the Shopping API.

    Args:
      batch: dict, JSON formatted for the API

    Returns:
      The response from the API call (JSON formatted)
    """
    request = self._service.products().custombatch(body=batch)
    return request.execute()


def _handle_response(
    response: Dict[str, Any], batch_number: int,
    batch_id_to_item_id: constants.BatchIdToItemId
) -> Tuple[List[str], List[failure.Failure]]:
  """Processes the response from an API call.

  Args:
    response: A response from Content API which includes product data. See
      https://developers.google.com/shopping-content/v2/reference/v2/products#resource
    batch_number: Identifier for this batch
    batch_id_to_item_id: A dictionary that maps batch ids to items ids. Content
      API responses will only return the batch id.

  Returns:
    A list of successful ids and failures parsed from the result of
    the Content API call
  """
  successful_item_ids = []
  item_failures = []
  response_kind = response.get('kind', '')

  if response_kind != 'content#productsCustomBatchResponse':
    logging.warning('Batch #%d: Invalid response format. Response: %s',
                    batch_number, response)
    # If the batch as a whole returned an error, log every id in the batch as
    # a failure.
    item_failures = [
        failure.Failure(item_id, 'API returned unexpected response.')
        for item_id in batch_id_to_item_id.values()
    ]
    return successful_item_ids, item_failures

  entries = response.get('entries', [])
  for entry in entries:
    batch_id = entry.get('batchId')
    item_id = batch_id_to_item_id.get(batch_id)
    response_errors = entry.get('errors')
    if response_errors:
      error_msg = (
          f'Code: '
          f'{response_errors.get("code", "Error code not found")}.'
          f' Message: '
          f'{response_errors.get("message", "Error message not found.")}')
      item_failures.append(failure.Failure(item_id, error_msg))
      logging.info('Batch #%d: Errors for batch entry %d. Error: %s',
                   batch_number, batch_id, response_errors)
    else:
      successful_item_ids.append(item_id)

  return successful_item_ids, item_failures


def suggest_retry(status_code: int) -> bool:
  """Check if a task should be retried or not.

  Args:
    status_code: Status code of Content API call response.

  Returns:
    Whether the task should be retried or not.
  """
  return status_code in _RETRY_STATUS_CODE
