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
"""Unit tests for ContentApiClient."""

import unittest
import unittest.mock as mock

from parameterized import parameterized

import constants
import content_api_client
import test_utils

DUMMY_BATCH_NUMBER = 0


class ModuleFunctionsTest(unittest.TestCase):

  @parameterized.expand([
      (constants.ShoppingApiErrorCodes.MALFORMED_REQUEST.value, False),
      (constants.ShoppingApiErrorCodes.AUTHENTICATION_ERROR.value, True),
      (constants.ShoppingApiErrorCodes.ACCESS_ERROR.value, True),
      (constants.ShoppingApiErrorCodes.RESOURCE_NOT_FOUND.value, False),
      (constants.ShoppingApiErrorCodes.DUPLICATE_CHANGES_DETECTED.value, False),
      (constants.ShoppingApiErrorCodes.QUOTA_LIMITS_REACHED.value, True),
      (constants.ShoppingApiErrorCodes.INTERNAL_SERVER_ERROR.value, True),
      (constants.ShoppingApiErrorCodes.OPERATION_NOT_PERMITTED.value, False),
      (constants.ShoppingApiErrorCodes.SOCKET_TIMEOUT.value, True),
  ])
  def test_suggest_retry(self, status_code, expected):
    self.assertEqual(expected, content_api_client.suggest_retry(status_code))


class ContentApiClientTest(unittest.TestCase):

  def setUp(self):
    super(ContentApiClientTest, self).setUp()
    self._api_service = mock.Mock()
    self._client = content_api_client.ContentApiClient(self._api_service)
    constants.MERCHANT_ID = test_utils.DUMMY_MERCHANT_ID

  @parameterized.expand([
      (constants.Method.INSERT, test_utils.SINGLE_ITEM_COUNT),
      (constants.Method.INSERT, test_utils.MULTIPLE_ITEM_COUNT),
      (constants.Method.DELETE, test_utils.SINGLE_ITEM_COUNT),
      (constants.Method.DELETE, test_utils.MULTIPLE_ITEM_COUNT),
  ])
  def test_process_items(self, method, num_rows):
    _, batch, batch_id_to_item_id, expected_response = test_utils.generate_test_data(
        method, num_rows)
    self._api_service.products.return_value.custombatch.return_value.execute.return_value = expected_response

    successful_item_ids, item_failures = self._client.process_items(
        batch, DUMMY_BATCH_NUMBER, batch_id_to_item_id, method)

    self._api_service.products.return_value.custombatch.return_value.execute.assert_called(
    )
    self.assertEqual(num_rows, len(successful_item_ids))
    self.assertEqual(0, len(item_failures))

  @parameterized.expand([
      (test_utils.SINGLE_ITEM_COUNT,),
      (test_utils.MULTIPLE_ITEM_COUNT,),
  ])
  def test_process_items_insert_returns_items_with_errors(self, num_rows):
    _, batch, batch_id_to_item_id, _ = test_utils.generate_test_data(
        constants.Method.INSERT, num_rows)
    response_with_errors = test_utils._generate_insert_response_with_errors(
        num_rows)
    self._api_service.products.return_value.custombatch.return_value.execute.return_value = response_with_errors

    successful_item_ids, item_failures = self._client.process_items(
        batch, DUMMY_BATCH_NUMBER, batch_id_to_item_id, constants.Method.INSERT)

    self._api_service.products.return_value.custombatch.return_value.execute.assert_called(
    )
    self.assertEqual(0, len(successful_item_ids))
    self.assertEqual(num_rows, len(item_failures))

  @parameterized.expand([
      (test_utils.SINGLE_ITEM_COUNT,),
      (test_utils.MULTIPLE_ITEM_COUNT,),
  ])
  def test_process_items_delete_returns_items_with_errors(self, num_rows):
    _, batch, batch_id_to_item_id, _ = test_utils.generate_test_data(
        constants.Method.DELETE, num_rows)
    response_with_errors = test_utils._generate_delete_response_with_errors(
        num_rows)
    self._api_service.products.return_value.custombatch.return_value.execute.return_value = response_with_errors

    successful_item_ids, item_failures = self._client.process_items(
        batch, DUMMY_BATCH_NUMBER, batch_id_to_item_id, constants.Method.DELETE)

    self._api_service.products.return_value.custombatch.return_value.execute.assert_called(
    )
    self.assertEqual(0, len(successful_item_ids))
    self.assertEqual(num_rows, len(item_failures))

  @parameterized.expand([
      (test_utils.SINGLE_ITEM_COUNT,),
      (test_utils.MULTIPLE_ITEM_COUNT,),
  ])
  def test_process_items_records_item_failures_when_response_invalid(
      self, num_rows):
    _, batch, batch_id_to_item_id, _ = test_utils.generate_test_data(
        constants.Method.INSERT, num_rows)
    response_with_errors = test_utils._generate_response_with_invalid_kind_value(
        num_rows)
    self._api_service.products.return_value.custombatch.return_value.execute.return_value = response_with_errors

    successful_item_ids, item_failures = self._client.process_items(
        batch, DUMMY_BATCH_NUMBER, batch_id_to_item_id, constants.Method.INSERT)

    self._api_service.products.return_value.custombatch.return_value.execute.assert_called(
    )
    self.assertEqual(0, len(successful_item_ids))
    self.assertEqual(num_rows, len(item_failures))
