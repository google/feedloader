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

"""Helper functions to create data for use in unit tests."""

import string
from typing import Any, Dict, List, Tuple

from google.cloud import bigquery

import constants

BATCH_NUMBER = 1
DUMMY_MERCHANT_ID = '1234567'
MULTIPLE_ITEM_COUNT = 2
SINGLE_ITEM_COUNT = 1

ROW_SCHEMA = {
    'google_merchant_id': 0,
    'item_id': 1,
    'title': 2,
    'description': 3,
    'google_product_category': 4,
    'product_types': 5,
    'link': 6,
    'image_link': 7,
    'additional_image_link': 8,
    'condition': 9,
    'availability': 10,
    'price': 11,
    'brand': 12,
    'gtin': 13,
    'mpn': 14,
    'shipping': 15,
    'loyalty_points': 16,
    'ads_redirect': 17,
    'color': 18,
    'size': 19,
    'custom_label_0': 20,
    'custom_label_1': 21,
    'custom_label_2': 22,
    'custom_label_3': 23,
    'custom_label_4': 24,
    'identifier_exists': 25
}

ROW_SCHEMA_LOCAL = {
    'google_merchant_id': 0,
    'store_code': 1,
    'item_id': 2,
    'price': 3,
    'quantity': 4,
    'availability': 5,
    'pickup_method': 6,
    'pickup_sla': 7,
    'sale_price': 8,
    'sale_price_effective_date': 9,
}


def generate_test_data(
    method: constants.Method,
    num_rows: int = 1,
    remove_merchant_id: bool = False,
) -> Tuple[
    List[bigquery.Row],
    constants.Batch,
    constants.BatchIdToItemId,
    Dict[str, Any],
]:
  """Generates a tuple containing a triplet of matching row, batch, response.

  The data format follows 'Product'.
  See details:
  https://developers.google.com/shopping-content/reference/rest/v2.1/products#Product

  Args:
    method: The API method for this batch (insert, delete)
    num_rows: The number of rows to generate
    remove_merchant_id: If true, set merchant_id to None

  Returns:
    A tuple containing row_data, batch, and response. The row_data represents
    data pulled directly from BigQuery in as Row objects. The batch data is a
    Content API request batch corresponding to the Row data but in JSON object
    form. The response is a JSON object which represents the expected response
    from the Content API call.
  """
  rows = []
  batch = {'entries': []}
  batch_id_to_item_id = dict()
  response = {u'kind': u'content#productsCustomBatchResponse', u'entries': []}
  for batch_id in range(0, num_rows):
    merchant_id, item, api_item = generate_item_dict_api_pair(
        feed_type=constants.FeedType.PRIMARY
    )
    if remove_merchant_id:
      merchant_id = None
    rows.append(
        bigquery.Row(
            (merchant_id, item['item_id'], item['title'], item['description'],
             item['google_product_category'], item['product_types'],
             item['link'], item['image_link'], item['additional_image_link'],
             item['condition'], item['availability'], item['price'],
             item['brand'], item['gtin'], item['mpn'], item['shipping'],
             item['loyalty_points'], item['ads_redirect'], item['color'],
             item['size'], item['custom_label_0'], item['custom_label_1'],
             item['custom_label_2'], item['custom_label_3'],
             item['custom_label_4'], item['identifier_exists']), ROW_SCHEMA))
    batch_id_to_item_id[batch_id] = item['item_id']
    if method == constants.Method.INSERT:
      batch['entries'].append({
          'batchId': batch_id,
          'merchantId': str(merchant_id),
          'method': method.value,
          'product': api_item
      })
      response['entries'].append({
          'batchId': batch_id,
          'kind': 'content#productsCustomBatchResponseEntry',
          'product': {
              'color': item['color'],
              'offerId': item['item_id'],
              'gtin': item['gtin'],
              'googleProductCategory': item['google_product_category'],
              'availability': item['availability'],
              'targetCountry': constants.TARGET_COUNTRY,
              'title': item['title'],
              'item_id': '{}:{}:{}:{}'.format(
                  constants.Channel.ONLINE.value,
                  constants.CONTENT_LANGUAGE,
                  constants.TARGET_COUNTRY,
                  item['item_id'],
              ),
              'customLabel1': item['custom_label_1'],
              'price': {
                  'currency': constants.TARGET_CURRENCY,
                  'value': item['price'],
              },
              'channel': api_item['channel'],
              'description': item['description'],
              'contentLanguage': api_item['contentLanguage'],
              'mpn': item['mpn'],
              'brand': item['brand'],
              'link': item['link'],
              'adsRedirect': item['ads_redirect'],
              'customLabel4': item['custom_label_4'],
              'customLabel3': item['custom_label_3'],
              'customLabel2': item['custom_label_2'],
              'condition': item['condition'],
              'customLabel0': item['custom_label_0'],
              'kind': 'content#product',
              'identifierExists': item['identifier_exists'],
              'imageLink': item['image_link'],
              'productTypes': [item['product_types']],
          },
      })
    elif method == constants.Method.DELETE:
      json_snippet = {
          'batchId': batch_id,
          'merchantId': str(merchant_id),
          'method': method.value,
          'productId': '{}:{}:{}:{}'.format(
              constants.Channel.ONLINE.value,
              constants.CONTENT_LANGUAGE,
              constants.TARGET_COUNTRY,
              item['item_id'],
          ),
      }
      batch['entries'].append(json_snippet)
      response['entries'].append(json_snippet)

  return rows, batch, batch_id_to_item_id, response


def generate_item_dict_api_pair(
    feed_type: constants.FeedType, **kwargs: Dict[str, Any]
) -> Tuple[str, constants.Product, constants.Product]:
  """Generate a pair of data objects for testing.

  Generate a pair of objects here that can be used to compare. Initially
  generate a base item that has all default values, then overwrite this with any
  passed arguments, into this function, then generate an API formatted object
  that is the complement to the base item.

  Args:
    feed_type: The feed type which is PRIMARY or LOCAL.
    **kwargs: A dictionary of parameters that will be used to overwrite any
      default values.

  Returns:
    A tuple containing a pair of objects that represent the dict format item
    of a Row from BigQuery and the expected resulting item that should be
    returned by the API mapping method.
  """
  merchant_id = DUMMY_MERCHANT_ID

  if feed_type == constants.FeedType.PRIMARY:
    item = {
        'google_merchant_id': merchant_id,
        'item_id': 'test id',
        'title': 'test title',
        'description': 'test description',
        'google_product_category': 'Test > Google > Product > Category',
        'product_types': 'Test > Product > Type',
        'link': 'https://test.example.co.jp/products/1/',
        'image_link': 'https://test.example.co.jp/products/1/image.jpg',
        'additional_image_link': None,
        'condition': 'new',
        'availability': 'in stock',
        'price': '100',
        'brand': 'Test Brand',
        'gtin': '12345678901234',
        'mpn': 'ABC1234',
        'shipping': None,
        'loyalty_points': None,
        'ads_redirect': 'https://redir.ex.co.jp/product/1/',
        'color': 'Blue',
        'size': 'M',
        'custom_label_0': None,
        'custom_label_1': None,
        'custom_label_2': None,
        'custom_label_3': None,
        'custom_label_4': None,
        'identifier_exists': True,
    }
  else:
    item = {
        'google_merchant_id': merchant_id,
        'store_code': 'ABCDE12345',
        'item_id': 'test_id',
        'price': '1000',
        'quantity': '10',
        'availability': 'in_stock',
        'pickup_method': 'buy',
        'pickup_sla': 'same_day',
        'sale_price': '900',
        'sale_price_effective_date': (
            '2024-01-01T09:00:00+09:00/2024-01-01T09:00:00+09:00'
        ),
    }

  if kwargs:
    for key, value in kwargs.items():
      item[key] = value

  if feed_type == constants.FeedType.PRIMARY:
    api_formatted_item = {
        'offerId': item['item_id'],
        'title': item['title'],
        'description': item['description'],
        'googleProductCategory': item['google_product_category'],
        'productTypes': [item['product_types']],
        'link': item['link'],
        'imageLink': item['image_link'],
        'additionalImageLinks': [],
        'condition': item['condition'],
        'availability': item['availability'],
        'price': {
            'currency': constants.TARGET_CURRENCY,
            'value': ''.join(c for c in item['price'] if c in string.digits),
        },
        'brand': item['brand'],
        'gtin': item['gtin'],
        'mpn': item['mpn'],
        'shipping': [],
        'loyaltyPoints': {},
        'adsRedirect': item['ads_redirect'],
        'color': item['color'],
        'sizes': [item['size']],
        'customLabel0': (
            item['custom_label_0'] if item['custom_label_0'] else ''
        ),
        'customLabel1': (
            item['custom_label_1'] if item['custom_label_1'] else ''
        ),
        'customLabel2': (
            item['custom_label_2'] if item['custom_label_2'] else ''
        ),
        'customLabel3': (
            item['custom_label_3'] if item['custom_label_3'] else ''
        ),
        'customLabel4': (
            item['custom_label_4'] if item['custom_label_4'] else ''
        ),
        'identifierExists': item['identifier_exists'],
        'contentLanguage': constants.CONTENT_LANGUAGE,
        'targetCountry': constants.TARGET_COUNTRY,
        'channel': constants.Channel.ONLINE.value,
    }
  else:
    api_formatted_item = {
        'storeCode': item['store_code'],
        'price': {
            'currency': constants.TARGET_CURRENCY,
            'value': ''.join(c for c in item['price'] if c in string.digits),
        },
        'salePrice': {
            'currency': constants.TARGET_CURRENCY,
            'value': ''.join(
                c for c in item['sale_price'] if c in string.digits
            ),
        },
        'salePriceEffectiveDate': item['sale_price_effective_date'],
        'availability': item['availability'],
        'quantity': int(item['quantity']),
        'pickupMethod': item['pickup_method'],
        'pickupSla': item['pickup_sla'],
    }

  empty_fields = [k for k in api_formatted_item if not api_formatted_item[k]]
  for empty_field in empty_fields:
    del api_formatted_item[empty_field]

  return merchant_id, item, api_formatted_item


def generate_insert_response_with_errors(
    feed_type: constants.FeedType, num_rows: int = 1
) -> Dict[str, Any]:
  """Generates a Content API insert response with errors.

  Args:
    feed_type: The feed type which is PRIMARY or LOCAL.
    num_rows: The number of rows to generate.

  Returns:
    A Content API response with errors (missing currency field).
  """
  if feed_type == constants.FeedType.PRIMARY:
    response = {'kind': 'content#productsCustomBatchResponse', 'entries': []}
    response_entry = 'content#productsCustomBatchResponseEntry'
  else:
    response = {
        'kind': 'content#localinventoryCustomBatchResponse',
        'entries': [],
    }
    response_entry = 'content#localinventoryCustomBatchResponseEntry'
  for batch_id in range(0, num_rows):
    response['entries'].append({
        'kind': response_entry,
        'batchId': batch_id,
        'errors': {
            'errors': [
                {
                    'domain': 'global',
                    'reason': 'required',
                    'message': (
                        '[price.currency] Required parameter: price.currency'
                    ),
                },
                {
                    'domain': 'content.ContentErrorDomain',
                    'reason': 'not_inserted',
                    'message': 'The item could not be inserted.',
                },
            ],
            'code': 400,
            'message': '[price.currency] Required parameter: price.currency',
        },
    })

  return response


def generate_delete_response_with_errors(num_rows: int = 1) -> Dict[str, Any]:
  """Generates a Content API delete response with errors.

  Args:
    num_rows: The number of rows to generate.

  Returns:
    A Content API response with errors (item not found).
  """
  response = {u'kind': u'content#productsCustomBatchResponse', u'entries': []}
  for batch_id in range(0, num_rows):
    response['entries'].append({
        'kind': 'content#productsCustomBatchResponseEntry',
        'batchId': batch_id,
        'errors': {
            'errors': [{
                'domain': 'global',
                'reason': 'notFound',
                'message': 'item not found'
            }],
            'code': 404,
            'message': 'item not found'
        }
    })

  return response


def generate_response_with_invalid_kind_value(
    num_rows: int = 1,
) -> Dict[str, Any]:
  """Generates a Content API response with an invalid kind value.

  Args:
    num_rows: The number of rows to generate.

  Returns:
    A Content API response with an invalid kind id.
  """
  response = {u'kind': u'content#invalid', u'entries': []}
  for _ in range(0, num_rows):
    response['entries'].append({'kind': 'content#invalid'})

  return response


def generate_test_data_local(
    method: constants.Method,
    num_rows: int = 1,
    remove_merchant_id: bool = False,
) -> Tuple[
    List[bigquery.Row],
    constants.Batch,
    constants.BatchIdToItemId,
    Dict[str, Any],
]:
  """Generates a tuple containing a triplet of matching row, batch, response.

  The data format follows 'LocalInventory'.
  See details:
  https://developers.google.com/shopping-content/reference/rest/v2.1/localinventory#LocalInventory

  Args:
    method: The API method for this batch (insert)
    num_rows: The number of rows to generate
    remove_merchant_id: If true, set merchant_id to None

  Returns:
    A tuple containing row_data, batch, and response. The row_data represents
    data pulled directly from BigQuery in as Row objects. The batch data is a
    Content API request batch corresponding to the Row data but in JSON object
    form. The response is a JSON object which represents the expected response
    from the Content API call.
  """
  rows = []
  batch = {'entries': []}
  batch_id_to_item_id = dict()
  response = {
      'kind': 'content#localinventoryCustomBatchResponse',
      'entries': [],
  }
  for batch_id in range(0, num_rows):
    merchant_id, item, api_item = generate_item_dict_api_pair(
        feed_type=constants.FeedType.LOCAL
    )
    if remove_merchant_id:
      merchant_id = None
    rows.append(
        bigquery.Row(
            (
                merchant_id,
                item['store_code'],
                item['item_id'],
                item['price'],
                item['quantity'],
                item['availability'],
                item['pickup_method'],
                item['pickup_sla'],
                item['sale_price'],
                item['sale_price_effective_date'],
            ),
            ROW_SCHEMA_LOCAL,
        )
    )
    batch_id_to_item_id[batch_id] = item['item_id']
    if method == constants.Method.INSERT:
      batch['entries'].append({
          'batchId': batch_id,
          'merchantId': str(merchant_id),
          'method': method.value,
          'productId': '{}:{}:{}:{}'.format(
              constants.Channel.LOCAL.value,
              constants.CONTENT_LANGUAGE,
              constants.TARGET_COUNTRY,
              item['item_id'],
          ),
          'localInventory': api_item,
      })
      response['entries'].append({
          'batchId': batch_id,
          'kind': 'content#localinventoryCustomBatchResponseEntry',
      })

  return rows, batch, batch_id_to_item_id, response
