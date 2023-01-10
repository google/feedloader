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

"""Unit tests for content_api_client."""

import logging
import unittest
import unittest.mock

from googleapiclient import discovery
from googleapiclient import http

import content_api_client

_MERCHANT_ID = 999999999
_SERVICE_NAME = 'content'
_SERVICE_VERSION = 'v2.1'
_API_RESPONSE_DIRECTORY = './test_content_api_responses/'


def _api_response(file_name):
  """Returns file path for mock API response file."""
  return _API_RESPONSE_DIRECTORY + file_name + '.json'


_API_RESPONSE_CONTENT_DISCOVERY = _api_response('content_discovery')
_API_RESPONSE_LIST_SUCCESS = _api_response('list_success')
_API_RESPONSE_LIST_EMPTY = _api_response('list_empty')
_API_RESPONSE_LIST_ERROR = _api_response('list_error')
_API_RESPONSE_DELETE_SUCCESS = _api_response('delete_success')
_API_RESPONSE_DELETE_ONE_SUCCESS_ONE_ERROR = _api_response(
    'delete_one_success_one_error')
_API_RESPONSE_DELETE_ERROR = _api_response('delete_error')

_HTTP_STATUS_200 = {'status': '200'}
_HTTP_STATUS_401 = {'status': '401'}


class ContentApiClientTest(unittest.TestCase):

  def setUp(self):
    super(ContentApiClientTest, self).setUp()
    mock_auth_http = http.HttpMock(
        filename=_API_RESPONSE_CONTENT_DISCOVERY, headers=_HTTP_STATUS_200)
    self._mock_service = discovery.build(
        _SERVICE_NAME,
        _SERVICE_VERSION,
        http=mock_auth_http,
        cache_discovery=False)
    self._client = content_api_client.ContentApiClient(self._mock_service)

  def test_list_products_with_success_api_call(self):
    expected_product_count = 2
    mock_http = http.HttpMock(
        filename=_API_RESPONSE_LIST_SUCCESS, headers=_HTTP_STATUS_200)
    products = self._client.list_products(
        merchant_id=_MERCHANT_ID, http_object=mock_http)
    self.assertEqual(expected_product_count, len(products))

  def test_list_products_with_error_api_call(self):
    mock_http = http.HttpMock(
        filename=_API_RESPONSE_LIST_ERROR, headers=_HTTP_STATUS_401)
    with self.assertRaises(content_api_client.ContentApiError):
      self._client.list_products(
          merchant_id=_MERCHANT_ID, http_object=mock_http)

  def test_delete_all_products_with_success_api_call(self):
    expected_error_count = 0
    mock_list_http = http.HttpMock(
        filename=_API_RESPONSE_LIST_SUCCESS, headers=_HTTP_STATUS_200)
    mock_delete_http = http.HttpMock(
        filename=_API_RESPONSE_DELETE_SUCCESS, headers=_HTTP_STATUS_200)
    error_count = self._client.delete_all_products(
        merchant_id=_MERCHANT_ID,
        list_http_object=mock_list_http,
        delete_http_object=mock_delete_http)
    self.assertEqual(expected_error_count, error_count)

  def test_delete_all_products_when_merchant_center_is_empty(self):
    expected_error_count = 0
    with unittest.mock.patch.object(
        content_api_client.ContentApiClient,
        '_delete_products',
        wraps=content_api_client.ContentApiClient._delete_products):
      mock_list_http = http.HttpMock(
          filename=_API_RESPONSE_LIST_EMPTY, headers=_HTTP_STATUS_200)
      error_count = self._client.delete_all_products(
          merchant_id=_MERCHANT_ID, list_http_object=mock_list_http)
      self.assertEqual(expected_error_count, error_count)
      self._client._delete_products.assert_not_called()

  def test_delete_all_products_with_error_list_api_call(self):
    with unittest.mock.patch.object(
        content_api_client.ContentApiClient,
        '_delete_products',
        wraps=content_api_client.ContentApiClient._delete_products):
      mock_list_http = http.HttpMock(
          filename=_API_RESPONSE_LIST_ERROR, headers=_HTTP_STATUS_401)
      with self.assertRaises(content_api_client.ContentApiError):
        self._client.delete_all_products(
            merchant_id=_MERCHANT_ID, list_http_object=mock_list_http)
      self._client._delete_products.assert_not_called()

  def test_delete_all_products_with_error_delete_api_call(self):
    mock_list_http = http.HttpMock(
        filename=_API_RESPONSE_LIST_SUCCESS, headers=_HTTP_STATUS_200)
    mock_delete_http = http.HttpMock(
        filename=_API_RESPONSE_DELETE_ERROR, headers=_HTTP_STATUS_401)
    with self.assertRaises(content_api_client.ContentApiError):
      self._client.delete_all_products(
          merchant_id=_MERCHANT_ID,
          list_http_object=mock_list_http,
          delete_http_object=mock_delete_http)

  def test_delete_all_products_when_one_product_is_not_deleted(self):
    expected_error_count = 1
    mock_list_http = http.HttpMock(
        filename=_API_RESPONSE_LIST_SUCCESS, headers=_HTTP_STATUS_200)
    mock_delete_http = http.HttpMock(
        filename=_API_RESPONSE_DELETE_ONE_SUCCESS_ONE_ERROR,
        headers=_HTTP_STATUS_200)
    error_count = self._client.delete_all_products(
        merchant_id=_MERCHANT_ID,
        list_http_object=mock_list_http,
        delete_http_object=mock_delete_http)
    self.assertEqual(expected_error_count, error_count)


if __name__ == '__main__':
  logging.disable(logging.CRITICAL)
  unittest.main(verbosity=2)
