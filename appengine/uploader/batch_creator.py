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

"""Creates a batch of product data to send to Content API for Shopping."""

from distutils import util
import logging
import numbers
import os
import string
from typing import Any, List, Tuple, Union

from google.cloud import bigquery

import constants

_FIELDS_TO_IGNORE = frozenset(['google_merchant_id'])
_PRODUCT_ID_FORMAT = '{channel}:{contentLanguage}:{targetCountry}:{offerId}'


def create_batch(
    batch_number: int, item_rows: List[bigquery.Row], method: constants.Method,
    channel: str
) -> Tuple[constants.Batch, List[str], constants.BatchIdToItemId]:
  """Processes a list of items into a batch ready to submit to the API.

  batch_number refers to the entire batch of items and batch_id refers to a
  single item within the batch.

  Args:
    batch_number: The id used to track this batch for logging purposes
    item_rows: List of rows from BigQuery items table.
    method: The operation to carry out on these items (Add Delete etc)
    channel: The ads destination channel. One of 'local' or 'online'.

  Returns:
    A tuple representing the batch object (dict), a list of skipped items and a
    dict that maps from batch id to item id.
  """
  batch = {'entries': []}
  skipped_item_ids = []
  batch_id_to_item_id = {}

  try:
    is_mca = util.strtobool(os.environ['IS_MCA'])
  except ValueError:
    is_mca = False

  for batch_id, item_row in enumerate(item_rows):
    if is_mca:
      if item_row['google_merchant_id']:
        merchant_id = item_row['google_merchant_id']
      else:
        item_id = item_row['item_id']
        logging.warning(
            'Account is MCA but missing or invalid value in field'
            'google_merchant_id for batch #%d item %s', batch_number, item_id)
        skipped_item_ids.append(item_id)
        continue
    else:
      merchant_id = os.environ['MERCHANT_ID']

    entry = {
        'batchId': batch_id,
        'merchantId': merchant_id,
        'method': method.value,
    }
    # Content API responses only return batch id, not item id.
    # So we have to store a map of batch ids to items ids.
    formatted_item = _convert_item_to_content_api_format(
        batch_number, item_row, channel)
    if method == constants.Method.INSERT:
      entry['product'] = formatted_item
    elif method == constants.Method.DELETE:
      formatted_product_id = _PRODUCT_ID_FORMAT.format(**formatted_item)
      entry['productId'] = formatted_product_id
    batch['entries'].append(entry)
    batch_id_to_item_id[batch_id] = item_row.get('item_id', '(Missing)')

  return batch, skipped_item_ids, batch_id_to_item_id


def _convert_item_to_content_api_format(batch_number: int,
                                        item_row: Union[bigquery.Row,
                                                        constants.Product],
                                        channel: str) -> constants.Product:
  """Converts item to the format required by the API.

  Args:
    batch_number: The id used to track this batch for logging purposes
    item_row: Dictionary representation of the input item
    channel: The ads destination channel. One of 'local' or 'online'.

  Returns:
    An item (dict) that has all fields (keys and values) mapped from the input
    format to the format needed to submit to the API.
  """
  api_formatted_item = {}
  for key, value in item_row.items():
    # Do not add this field if we should ignore
    if key in _FIELDS_TO_IGNORE:
      continue
    try:
      new_key, new_value = _convert_feed_field_to_api_field(key, value)
      if _has_valid_value(new_key, new_value):
        api_formatted_item[new_key] = new_value
    except ValueError as e:
      logging.debug('Error parsing batch #%d item %s: %s', batch_number,
                    item_row['item_id'], str(e))
  api_formatted_item['contentLanguage'] = constants.CONTENT_LANGUAGE
  api_formatted_item['targetCountry'] = constants.TARGET_COUNTRY
  api_formatted_item['channel'] = channel

  return api_formatted_item


def _convert_feed_field_to_api_field(original_key: str,
                                     original_value: str) -> Tuple[str, str]:
  """Converts attribute from feed format to API format.

  Args:
    original_key: The name of the field in the input data
    original_value: The value of the field in the input data

  Returns:
    Tuple that represents the field name used by the API (key) and the value of
    that field as expected in API format (new_value).
  """
  modified_key = _snake_to_camel_case(original_key)
  if modified_key in ('size', 'additionalImageLink', 'productType',
                      'includedDestination', 'excludedDestination'):
    modified_key = modified_key + 's'

  if modified_key in ('sizes', 'additionalImageLinks', 'productTypes',
                      'includedDestinations', 'excludedDestinations'):
    # Parse an attribute with repeated values.
    if original_value:
      modified_value = [
          element.strip() for element in original_value.split(',')
      ]
    else:
      modified_value = []
  elif modified_key == 'itemId':
    modified_key = 'offerId'
    modified_value = original_value
  elif modified_key in ('price', 'salePrice'):
    modified_value = {
        'currency': constants.TARGET_CURRENCY,
        'value': _strip_unwanted_chars(original_value)
    }
  elif modified_key == 'shipping':
    modified_value = []
  elif modified_key == 'loyaltyPoints':
    modified_value = {}
  elif modified_key == 'adwordsRedirect':
    modified_key = 'adsRedirect'
    modified_value = original_value
  else:
    modified_value = original_value if original_value is not None else ''
  return modified_key, modified_value


def _snake_to_camel_case(original_text: str) -> str:
  """Converts attribute name from snake to camel case."""
  if original_text:
    original_text = original_text[0].lower() + original_text[1:]
  components = original_text.replace(' ', '_').split('_')
  if len(components) > 1:
    return (components[0].lower() +
            ''.join(token.capitalize() for token in components[1:]))
  else:
    return components[0]


def _strip_unwanted_chars(price: Union[int, str]) -> str:
  """Returns price text with all unnecessary chars stripped (nonnumeric etc).

  Examples:
    "100" should return "100"
    "100 yen" should return "100"
    "10,000" should return "10000"

  Args:
    price: The raw value of the price data.

  Returns:
    String that represents the price with currency and other unnecessary
    punctuation removed.
  """
  return ''.join(char for char in str(price) if char in string.digits)


def _has_valid_value(key: str, value: Any) -> bool:
  """Checks if the field has a usable value.

  A field should be ignored when the value has no actual data (e.g. numeric 0 is
  meaningful data but None is not meaningful).

  This method returns False if the given value is either of None, '', [] or {}.
  If the value is 0, False, or other values then it returns True.
  If the key is either "price" or "salePrice" and the dictionary values for
  "value" or "currency" are empty, it returns False.

  Args:
    key: The key of the given field.
    value: The value of given field.

  Returns:
    Whether the field should be ignored or not.
  """
  if key in ('price', 'salePrice'):
    if isinstance(value, dict) and value.get('currency') and value.get('value'):
      return True
    else:
      return False

  if value:
    return True
  elif isinstance(value, numbers.Number) or isinstance(value, bool):
    return True
  else:
    return False
