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

"""Content API for Shopping client for end to end test."""

import logging

import google_auth_httplib2
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient import errors
from googleapiclient import http

_APPLICATION_NAME = 'End to End Test'
_CONTENT_API_SCOPE = 'https://www.googleapis.com/auth/content'
_SERVICE_NAME = 'content'
_SERVICE_VERSION = 'v2.1'


class ContentApiClient(object):
  """Content API for Shopping client.

  Since this class is made for end to end testing only, the number of products
  is limited to 1,000.

  Attributes:
    service: A Resource object with methods for interacting with the API.
  """

  def __init__(self, service):
    self._service = service

  @classmethod
  def from_service_account_json(cls, service_account_path):
    """Factory to retrieve JSON credentials while creating client.

    This method initializes API service and configuration for the Content API
    for Shopping.

    Args:
      service_account_path: String, path of the service account configuration
        file.

    Returns:
      The client created with the retrieved JSON credentials.
    """
    credentials = service_account.Credentials.from_service_account_file(
        service_account_path, scopes=[_CONTENT_API_SCOPE])
    auth_http = google_auth_httplib2.AuthorizedHttp(
        credentials,
        http=http.set_user_agent(http.build_http(), _APPLICATION_NAME))
    service = discovery.build(
        _SERVICE_NAME, _SERVICE_VERSION, http=auth_http, cache_discovery=False)
    return ContentApiClient(service=service)

  def list_products(self, merchant_id, http_object=None):
    """Lists all the products in the Merchant Center.

    Args:
      merchant_id: Integer, Merchant ID.
      http_object: httplib2.Http, an http object to be used in place of the one
        the HttpRequest request object was constructed with.

    Returns:
      List of products in the Merchant Center.

    Raises:
      ContentApiError: An error occurred calling Content API for Shopping.
    """
    request = self._service.products().list(merchantId=merchant_id)
    try:
      result = request.execute(http=http_object)
      products = result.get('resources', [])
      return products
    except errors.HttpError:
      logging.exception('Merchant Center #%d returned an error for list method',
                        merchant_id)
      raise ContentApiError('List method error', merchant_id)

  def delete_all_products(self,
                          merchant_id,
                          list_http_object=None,
                          delete_http_object=None):
    """Deletes all the products in the Merchant Center.

    Args:
      merchant_id: String, Merchant ID.
      list_http_object: httplib2.Http, an http object to be used in place of the
        one the HttpRequest request object was constructed with. This is for
        list method.
      delete_http_object: httplib2.Http, an http object to be used in place of
        the one the HttpRequest request object was constructed with. This is for
        delete method.

    Returns:
      Integer, number of products failed to be deleted.

    Raises:
      ContentApiError: An error occurred calling Content API for Shopping.
    """
    try:
      products = self.list_products(
          merchant_id=merchant_id, http_object=list_http_object)
    except ContentApiError:
      logging.error('delete_all_products failed since list_products method '
                    'raised an exception for Merchant Center #%d',
                    merchant_id)
      raise
    if not products:
      logging.info('Delete method was not called since Merchant Center #%d '
                   'did not return products for list method',
                   merchant_id)
      return 0
    try:
      entries = self._delete_products(
          merchant_id=merchant_id,
          products=products,
          http_object=delete_http_object)
    except ContentApiError:
      logging.error('delete_all_products failed since _delete_products method '
                    'raised an exception for Merchant Center #%d',
                    merchant_id)
      raise
    error_count = 0
    for entry in entries:
      response_errors = entry.get('errors')
      if response_errors:
        error_count += 1
        logging.error('Errors for entry #%d: %s', entry['batchId'],
                      response_errors)
    if error_count:
      logging.error('Failure: %d of products have not been deleted',
                    error_count)
    else:
      logging.info('Success: All products have been successfully deleted')
    return error_count

  def _delete_products(self, merchant_id, products, http_object=None):
    """Sends custombatch request to delete products.

    Args:
      merchant_id: Integer, Merchant ID.
      products: List, list of products.
      http_object: httplib2.Http, an http object to be used in place of the one
        the HttpRequest request object was constructed with.

    Returns:
      Response for a custombatch request.

    Raises:
      ContentApiError: An error occurred calling Content API for Shopping.
    """
    batch = {'entries': []}
    for index, product in enumerate(products):
      if 'id' in product.keys():
        entry = {
            'batchId': index,
            'merchantId': merchant_id,
            'method': 'delete',
            'productId': product['id']
        }
        logging.info('Entry #%d: %s', index, entry)
        batch['entries'].append(entry)
    request = self._service.products().custombatch(body=batch)
    try:
      result = request.execute(http=http_object)
      entries = result.get('entries')
      return entries
    except errors.HttpError:
      logging.exception(
          'Merchant Center #%d returned an error for delete method',
          merchant_id)
      raise ContentApiError('Delete method error', merchant_id)


class ContentApiError(Exception):
  """Error from Content API for Shopping.

  Attributes:
    message: String, Error message.
    merchant_id: Integer, Merchant ID.
  """

  def __init__(self, message, merchant_id):
    super(ContentApiError, self).__init__(message)
    self.merchant_id = merchant_id
