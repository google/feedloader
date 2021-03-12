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

"""Unit tests for shoptimizer_client."""

import unittest
import unittest.mock as mock

import requests

import constants
import shoptimizer_client
import test_utils

BATCH_NUMBER = 0
METHOD = constants.Method.INSERT
OPERATION = constants.Operation.UPSERT
SHOPTIMIZER_API_RESPONSE_SUCCESS = """
{
    "optimization-results": {
        "mpn-optimizer": {
            "error_msg": "",
            "num_of_products_optimized": 1,
            "result": "success"
        }
    },
    "optimized-data": {
        "entries": [
            {
                "batchId": 1111,
                "merchantId": 1234567,
                "method": "insert",
                "product": {
                    "ageGroup": "adult",
                    "availability": "in stock",
                    "availabilityDate": "2019-01-25T13:00:00-08:00",
                    "brand": "Google",
                    "channel": "online",
                    "color": "black",
                    "condition": "new",
                    "contentLanguage": "en",
                    "description": "The Black Google Tee is available in unisex sizing and features a retail fit.",
                    "destinations": [
                        {
                            "destinationName": "Shopping",
                            "intention": "required"
                        }
                    ],
                    "gender": "male",
                    "googleProductCategory": "1604",
                    "gtin": "608802531656",
                    "imageLink": "https://shop.example.com/.../images/GGOEGXXX1100.jpg",
                    "itemGroupId": "google_tee",
                    "kind": "content#product",
                    "link": "http://my.site.com/blacktee/",
                    "offerId": "1111111111",
                    "price": {
                        "currency": "USD",
                        "value": "21.99"
                    },
                    "sizes": [
                        "Large"
                    ],
                    "source": "api",
                    "targetCountry": "US",
                    "title": "Google Tee Black"
                }
            }
        ]
    },
    "plugin-results": {
        "my-plugin": {
            "error_msg": "",
            "num_of_products_optimized": 0,
            "result": "success"
        }
    }
}
"""


class MockResponse:
  """Used to represent an HTTP response in mocked HTTP calls."""

  def __init__(self, status_code, text):
    """Inits MockResponse.

    Args:
      status_code: An HTTP status code.
      text: Data returned in the HTTP response.
    """
    self.status_code = status_code
    self.text = text

  def raise_for_status(self):
    """Required to simulate requests.Response class."""
    pass


@mock.patch('shoptimizer_client.requests')
class ShoptimizerClientTest(unittest.TestCase):

  def setUp(self):
    super(ShoptimizerClientTest, self).setUp()
    self.client = shoptimizer_client.ShoptimizerClient(BATCH_NUMBER, OPERATION)

  def test_successful_response_returns_optimized_batch(self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.request.return_value = _create_mock_response(
        200, SHOPTIMIZER_API_RESPONSE_SUCCESS)

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      optimized_batch = self.client.shoptimize(original_batch)
      optimized_product = optimized_batch['entries'][0]['product']

      self.assertNotIn('mpn', optimized_product)
      self.assertNotEqual(original_batch, optimized_batch)

  def test_request_includes_configuration_parameters(self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.request.return_value = _create_mock_response(
        200, SHOPTIMIZER_API_RESPONSE_SUCCESS)

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      self.client.shoptimize(original_batch)

      self.assertIn('lang', mocked_requests.request.call_args[1]['params'])
      self.assertIn('country', mocked_requests.request.call_args[1]['params'])
      self.assertIn('currency', mocked_requests.request.call_args[1]['params'])

  def test_config_file_not_found_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    with mock.patch('builtins.open', side_effect=FileNotFoundError):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_empty_shoptimizer_url_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)

    with mock.patch('shoptimizer_client.constants') as mocked_constants:
      mocked_constants.SHOPTIMIZER_BASE_URL = ''
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_empty_batch_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    original_batch = {}

    returned_batch = self.client.shoptimize(original_batch)

    self.assertEqual(original_batch, returned_batch)
    mocked_requests.assert_not_called()

  def test_empty_optimization_params_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    with mock.patch('builtins.open',
                    mock.mock_open(read_data='')), mock.patch('json.load'):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_optimization_params_json_invalid_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    with mock.patch(
        'builtins.open',
        mock.mock_open(
            read_data='{"mpn-optimizer": "False" "identity-optimizer": "True"}')
    ), mock.patch('json.load'):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_no_true_optimization_param_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    with mock.patch(
        'builtins.open',
        mock.mock_open(
            read_data='{"mpn-optimizer": "False", "identity-optimizer": "False"}'
        )), mock.patch('json.load'):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_batch_invalid_json_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    original_batch = {'invalid json'}
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    returned_batch = self.client.shoptimize(original_batch)

    self.assertEqual(original_batch, returned_batch)
    mocked_requests.assert_not_called()

  def test_get_jwt_exception_does_not_call_api_and_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        side_effect=requests.exceptions.RequestException(
            'Token server connection error')):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)
      mocked_requests.assert_not_called()

  def test_shoptimizer_request_exception_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    mocked_requests.exceptions.RequestException = requests.exceptions.RequestException
    mocked_requests.request.side_effect = requests.exceptions.RequestException(
        'Shoptimizer server connection error')

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)

  def test_shoptimizer_response_contains_error_msg_returns_original_batch(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    shoptimizer_api_response_failure_bad_request = """{
        "error-msg": "Request must contain 'entries' as a key.",
        "optimization-results": {},
        "optimized-data": {},
        "plugin-results": {}
    }"""
    mocked_requests.request.return_value = _create_mock_response(
        400, shoptimizer_api_response_failure_bad_request)

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      returned_batch = self.client.shoptimize(original_batch)

      self.assertEqual(original_batch, returned_batch)

  def test_shoptimizer_builtin_optimizer_errors_are_logged(
      self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    shoptimizer_api_response_with_builtin_optimizer_error = """
    {
      "optimization-results": {
        "mpn-optimizer": {
          "error_msg": "An unexpected error occurred",
          "num_of_products_optimized": 0,
          "result": "failure"
        }
      },
      "optimized-data": {
        "entries": [{
          "batchId": 1111,
          "merchantId": 1234567,
          "method": "insert"
        }]
      }
    }
    """
    mocked_requests.request.return_value = _create_mock_response(
        200, shoptimizer_api_response_with_builtin_optimizer_error)

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      with self.assertLogs(level='ERROR') as log:
        self.client.shoptimize(original_batch)

        self.assertIn(
            f'ERROR:root:Request for batch #0 with operation upsert encountered '
            'an error when running optimizer mpn-optimizer. Error: An unexpected error occurred',
            log.output)

  def test_shoptimizer_plugin_errors_are_logged(self, mocked_requests):
    _, original_batch, _, _ = test_utils.generate_test_data(METHOD)
    shoptimizer_api_response_with_builtin_optimizer_error = """
    {
      "plugin-results": {
        "my-plugin": {
          "error_msg": "An unexpected error occurred",
          "num_of_products_optimized": 0,
          "result": "failure"
        }
      },
      "optimized-data": {
        "entries": [{
          "batchId": 1111,
          "merchantId": 1234567,
          "method": "insert"
        }]
      }
    }
    """
    mocked_requests.request.return_value = _create_mock_response(
        200, shoptimizer_api_response_with_builtin_optimizer_error)

    with mock.patch(
        'shoptimizer_client.ShoptimizerClient._get_jwt',
        return_value='jwt data'):
      with self.assertLogs(level='ERROR') as log:
        self.client.shoptimize(original_batch)

        self.assertIn(
            f'ERROR:root:Request for batch #0 with operation upsert encountered '
            'an error when running optimizer my-plugin. Error: An unexpected error occurred',
            log.output)


def _create_mock_response(status: int, response_data: str) -> MockResponse:
  """Creates a MockResponse object that can be used to simulate HTTP responses.

  Args:
    status: An HTTP status code.
    response_data: Data returned in the HTTP response.

  Returns:
    A MockResponse object containing the specified status code and response
    data.
  """
  return MockResponse(status, response_data)
