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
"""Client to send product data to the Shoptimizer (optimization) API and parse the results."""

import json
import logging
from typing import Any, Dict

import requests

import constants

_CONFIG_FILE_PATH = 'config/shoptimizer_config.json'
_ERROR_MSG_TEMPLATE = ('Request for batch #%d with operation %s encountered an '
                       'error: %s. Error: %s')
_METADATA_SERVER_TOKEN_URL = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='


class ShoptimizerClient(object):
  """Client to send product data to the Shoptimizer (optimization) API and parse the results."""

  def __init__(self, batch_number: int, operation: constants.Operation):
    """Inits ShoptimizerClient.

    Args:
      batch_number: The number that identifies this batch.
      operation: The operation to be performed on this batch (upsert, delete,
        prevent_expiring).
    """
    self._batch_number = batch_number
    self._operation = operation
    self._optimization_params = _load_optimization_params(
        self._batch_number, self._operation)

  def shoptimize(self, batch: constants.Batch) -> constants.Batch:
    """Optimizes a batch of product data by sending it to the Shoptimizer (optimization) API.

    Args:
      batch: The batch of product data to be optimized.

    Returns:
      The optimized batch of product data if no errors encountered,
      or the original batch of product data otherwise.
    """
    if not self._is_input_valid(batch):
      return batch

    try:
      response_dict = self._send_to_shoptimizer(batch)
    except (TypeError, requests.exceptions.RequestException, ValueError):
      return batch

    # Checks for some top-level failure in response
    # (received response in correct format without exceptions,
    #  but something went wrong in Shoptimizer)
    if response_dict.get('error-msg', ''):
      logging.error(_ERROR_MSG_TEMPLATE, self._batch_number,
                    self._operation.value,
                    'Encountered an error in the Shoptimizer API response',
                    response_dict['error-msg'])
      return batch

    self._log_results(response_dict)

    return response_dict.get('optimized-data', batch)

  def _is_input_valid(self, batch: constants.Batch) -> bool:
    """Checks input parameters are valid.

    Args:
      batch: The batch of product data to be optimized.

    Returns:
      True if the input is valid, False otherwise.
    """
    if not constants.SHOPTIMIZER_BASE_URL:
      logging.warning(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Shoptimizer API URL is not set. '
          'Check the SHOPTIMIZER_URL environment variable is correctly set', '')
      return False

    if not batch:
      logging.warning(_ERROR_MSG_TEMPLATE, self._batch_number,
                      self._operation.value,
                      'Batch was empty. Shoptimizer API not called', '')
      return False

    if not self._optimization_params:
      logging.info(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Optimization parameters were empty. Shoptimizer API not called')
      return False

    if 'true' not in [
        val.lower() for val in self._optimization_params.values()
    ]:
      logging.info(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'no true optimization parameter. Shoptimizer API not called.', '')
      return False

    return True

  def _send_to_shoptimizer(self, batch) -> Dict[str, Any]:
    """Logs errors returned by individual Shoptimizer API optimizers.

    Args:
      batch: The batch of product data to be optimized.

    Returns:
      A dictionary containing the results of the Shoptimizer API call.
    """
    try:
      batch_as_json = json.dumps(batch)
    except TypeError as type_error:
      logging.exception(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Failed to convert batch to JSON. Shoptimizer API not called',
          type_error)
      raise

    try:
      jwt = self._get_jwt()
    except requests.exceptions.RequestException:
      raise

    try:
      headers = {
          'Authorization': f'bearer {jwt}',
          'Content-Type': 'application/json'
      }
      response = requests.request(
          'POST',
          constants.SHOPTIMIZER_ENDPOINT,
          data=batch_as_json,
          headers=headers,
          params=self._optimization_params)
      response.raise_for_status()
      response_dict = json.loads(response.text)
    except requests.exceptions.RequestException as request_exception:
      logging.exception(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Did not receive a successful response from the Shoptimizer API',
          request_exception)
      raise
    except ValueError as value_error:
      logging.exception(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Failed to deserialize JSON returned from Shoptimizer API',
          value_error)
      raise

    return response_dict

  def _get_jwt(self) -> str:
    """Retrieves a JSON web token from the Google metadata server for Cloud Run authentication.

    Returns:
        A JSON web token that can be used for Cloud Run authentication.
    """
    try:
      token_request_url = _METADATA_SERVER_TOKEN_URL + constants.SHOPTIMIZER_BASE_URL
      token_request_headers = {'Metadata-Flavor': 'Google'}

      # Fetches the token
      response = requests.get(token_request_url, headers=token_request_headers)
      response.raise_for_status()
      jwt = response.content.decode('utf-8')
    except requests.exceptions.RequestException as request_exception:
      logging.exception(
          _ERROR_MSG_TEMPLATE, self._batch_number, self._operation.value,
          'Failed get an authentication JWT. Shoptimizer API not called',
          request_exception)
      raise

    return jwt

  def _log_results(self, response_dict: Dict[str, Any]) -> None:
    """Logs the results of the call to the Shoptimizer API.

    Args:
      response_dict: The results of the call to the Shoptimizer API.
    """
    optimization_results = response_dict.get('optimization-results', '')
    plugin_results = response_dict.get('plugin-results', '')

    self._log_optimizer_error_msgs(optimization_results)
    self._log_optimizer_error_msgs(plugin_results)

    logging.info(
        'Shoptimizer API finished running for batch #%d with operation %s. '
        'Optimizer Results: %s | Plugin Results: %s', self._batch_number,
        self._operation.value, optimization_results, plugin_results)

  def _log_optimizer_error_msgs(
      self, shoptimizer_results: Dict[str, Dict[str, Any]]) -> None:
    """Logs errors returned by individual Shoptimizer API optimizers.

    Args:
      shoptimizer_results: The results of each individual optimizer returned
        from the Shoptimizer API.
    """
    if not shoptimizer_results:
      return

    for optimizer_name, optimizer_results in shoptimizer_results.items():
      if optimizer_results.get('result', '') == 'failure':
        logging.error(
            'Request for batch #%d with operation %s encountered an error when '
            'running optimizer %s. Error: %s', self._batch_number,
            self._operation.value, optimizer_name,
            optimizer_results.get('error_msg', '(error_msg missing)'))


def _load_optimization_params(batch_number: int,
                              operation: constants.Operation) -> Dict[str, str]:
  """Loads optimization parameters for the Shoptimizer API.

  Args:
    batch_number: The number that identifies this batch.
    operation: The operation to be performed on this batch (upsert, delete,
      prevent_expiring).

  Returns:
    The optimization parameters for the Shoptimizer API.
  """
  try:
    with open(_CONFIG_FILE_PATH) as shoptimizer_config:
      optimization_params = json.loads(shoptimizer_config.read())
  except OSError as os_error:
    logging.exception(
        _ERROR_MSG_TEMPLATE, batch_number, operation.value,
        'Failed to read the shoptimizer config. '
        'Check config/shoptimizer_config.json exists and has read permissions. '
        'Shoptimizer API not called', os_error)
    raise
  except ValueError as value_error:
    logging.exception(
        _ERROR_MSG_TEMPLATE, batch_number, operation.value,
        'Failed to read the shoptimizer config. '
        'Check config/shoptimizer_config.json is valid JSON. '
        'Shoptimizer API not called', value_error)
    raise

  return optimization_params
