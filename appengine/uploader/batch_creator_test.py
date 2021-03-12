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

"""Unit tests for Batch Creator."""

import unittest

from parameterized import parameterized

import batch_creator
import constants
import test_utils

ITEM_WITH_DESCRIPTION_EMPTY = {'description': ''}
ITEM_WITH_CONDITION_USED = {'condition': 'used'}
ITEM_WITH_MULTIPLE_COLORS = {'color': 'Red/Blue/Green'}
ITEM_WITH_MULTIPLE_SIZES = {'size': 'XS S M'}
ITEM_WITH_SHIPPING_COST = {'shipping': '100'}
ITEM_WITH_CUSTOM_LABEL_SET = {'custom_label_0': 'testing custom label'}
ITEM_WITH_LOYALTY_POINTS = {'loyalty_points': '100'}
ITEM_WITH_TITLE_AND_SHIPPING = {'title': 'Ship!', 'shipping': 'JP:::650 JPY'}
ITEM_WITH_CURRENCY_IN_PRICE = {'price': '100 JPY'}
ITEM_WITH_GOOGLE_MERCHANT_ID = {'google_merchant_id': '0000'}
ITEM_WITH_PRODUCT_TYPES = {'product_types': 'clothing'}
ITEM_WITH_ADS_REDIRECT = {'ads_redirect': 'https://google.com'}


class BatchCreatorTest(unittest.TestCase):

  def setUp(self):
    super(BatchCreatorTest, self).setUp()
    constants.MERCHANT_ID = test_utils.DUMMY_MERCHANT_ID

  @parameterized.expand([
      ('', ''),
      ('a', 'a'),
      ('A', 'a'),
      ('abc', 'abc'),
      ('a_b_c', 'aBC'),
      ('test value', 'testValue'),
      ('TEST VALUE', 'testValue'),
      ('Test Value', 'testValue'),
      ('a b_c', 'aBC'),
  ])
  def test_snake_to_camel_case(self, test_value, expected_value):
    result = batch_creator._snake_to_camel_case(test_value)

    self.assertEqual(expected_value, result)

  @parameterized.expand([('custom_label_0', '', 'customLabel0', ''),
                         ('custom_label_0', None, 'customLabel0', ''),
                         ('shipping', '', 'shipping', []),
                         ('shipping', '123', 'shipping', []),
                         ('size', 'S', 'sizes', ['S']),
                         ('size', 'S,M,L', 'sizes', ['S', 'M', 'L']),
                         ('size', 'S M L', 'sizes', ['S M L']),
                         ('item_id', 'asdf', 'offerId', 'asdf'),
                         ('price', '100', 'price', {
                             'currency': constants.TARGET_CURRENCY,
                             'value': '100'
                         }),
                         ('price', '100JPY', 'price', {
                             'currency': constants.TARGET_CURRENCY,
                             'value': '100'
                         }),
                         ('price', '100 JPY', 'price', {
                             'currency': constants.TARGET_CURRENCY,
                             'value': '100'
                         }),
                         ('sale_price', '100', 'salePrice', {
                             'currency': constants.TARGET_CURRENCY,
                             'value': '100'
                         }),
                         ('sale_price', '100 JPY', 'salePrice', {
                             'currency': constants.TARGET_CURRENCY,
                             'value': '100'
                         }), ('loyalty_points', '100', 'loyaltyPoints', {}),
                         ('loyalty_points', '', 'loyaltyPoints', {}),
                         ('product_type', 'clothing,shoes', 'productTypes',
                          ['clothing', 'shoes']),
                         ('product_type', '', 'productTypes', []),
                         ('adwords_redirect', 'https://google.com',
                          'adsRedirect', 'https://google.com')])
  def test_convert_feed_field_to_api_field(self, key, value, new_key_expected,
                                           new_value_expected):
    new_key, new_value = batch_creator._convert_feed_field_to_api_field(
        key, value)

    self.assertEqual(new_key_expected, new_key)
    self.assertEqual(new_value_expected, new_value)

  @parameterized.expand([
      (ITEM_WITH_DESCRIPTION_EMPTY,),
      (ITEM_WITH_CONDITION_USED,),
      (ITEM_WITH_MULTIPLE_COLORS,),
      (ITEM_WITH_MULTIPLE_SIZES,),
      (ITEM_WITH_SHIPPING_COST,),
      (ITEM_WITH_CUSTOM_LABEL_SET,),
      (ITEM_WITH_LOYALTY_POINTS,),
      (ITEM_WITH_TITLE_AND_SHIPPING,),
      (ITEM_WITH_CURRENCY_IN_PRICE,),
      (ITEM_WITH_GOOGLE_MERCHANT_ID,),
      (ITEM_WITH_DESCRIPTION_EMPTY,),
      (ITEM_WITH_CONDITION_USED,),
      (ITEM_WITH_MULTIPLE_COLORS,),
      (ITEM_WITH_MULTIPLE_SIZES,),
      (ITEM_WITH_SHIPPING_COST,),
      (ITEM_WITH_CUSTOM_LABEL_SET,),
      (ITEM_WITH_LOYALTY_POINTS,),
      (ITEM_WITH_TITLE_AND_SHIPPING,),
      (ITEM_WITH_CURRENCY_IN_PRICE,),
      (ITEM_WITH_GOOGLE_MERCHANT_ID,),
      (ITEM_WITH_PRODUCT_TYPES,),
      (ITEM_WITH_ADS_REDIRECT,),
  ])
  def test_convert_item_to_content_api_format(self, fields):
    batch_id = 0
    _, item, expected_api_formatted_item = test_utils.generate_item_dict_api_pair(
        **fields)

    api_formatted_item = batch_creator._convert_item_to_content_api_format(
        batch_id, item)

    self.assertEqual(expected_api_formatted_item, api_formatted_item)

  @parameterized.expand([('price', None, 'price'),
                         ('sale_price', None, 'salePrice'),
                         ('title', None, 'title')])
  def test_convert_item_to_content_api_format_removes_a_field_when_its_value_is_invalid(
      self, original_field_name, original_field_value, formatted_field_name):
    batch_id = 0
    item = {original_field_name: original_field_value}

    api_formatted_item = batch_creator._convert_item_to_content_api_format(
        batch_id, item)

    self.assertNotIn(formatted_field_name, api_formatted_item)

  @parameterized.expand([
      (True, test_utils.SINGLE_ITEM_COUNT),
      (True, test_utils.MULTIPLE_ITEM_COUNT),
      (False, test_utils.SINGLE_ITEM_COUNT),
      (False, test_utils.MULTIPLE_ITEM_COUNT),
  ])
  def test_create_insert_batch(self, is_mca, num_rows):
    constants.IS_MCA = is_mca
    method = constants.Method.INSERT
    batch_id = test_utils.BATCH_NUMBER
    item_rows, expected_batch, _, _ = test_utils.generate_test_data(
        method, num_rows)

    actual_batch, _, _ = batch_creator.create_batch(batch_id, item_rows, method)

    self.assertEqual(expected_batch, actual_batch)

  @parameterized.expand([
      (True, test_utils.SINGLE_ITEM_COUNT),
      (True, test_utils.MULTIPLE_ITEM_COUNT),
      (False, test_utils.SINGLE_ITEM_COUNT),
      (False, test_utils.MULTIPLE_ITEM_COUNT),
  ])
  def test_create_delete_batch(self, is_mca, num_rows):
    constants.IS_MCA = is_mca
    method = constants.Method.DELETE
    batch_id = test_utils.BATCH_NUMBER
    item_rows, expected_batch, _, _ = test_utils.generate_test_data(
        method, num_rows)

    actual_batch, _, _ = batch_creator.create_batch(batch_id, item_rows, method)

    self.assertEqual(expected_batch, actual_batch)

  def test_create_batch_returns_skipped_items_when_merchant_id_missing(self):
    constants.IS_MCA = True
    method = constants.Method.INSERT
    batch_number = test_utils.BATCH_NUMBER
    remove_merchant_ids = True
    item_rows, _, _, _ = test_utils.generate_test_data(
        method, test_utils.MULTIPLE_ITEM_COUNT, remove_merchant_ids)

    _, skipped_item_ids, _ = batch_creator.create_batch(batch_number, item_rows,
                                                        method)

    self.assertEqual(test_utils.MULTIPLE_ITEM_COUNT, len(skipped_item_ids))
    self.assertEqual('test id', skipped_item_ids[0])
    self.assertEqual('test id', skipped_item_ids[1])

  def test_create_batch_returns_batch_to_item_id_dict(self):
    constants.IS_MCA = True
    method = constants.Method.INSERT
    batch_id = test_utils.BATCH_NUMBER
    item_rows, _, _, _ = test_utils.generate_test_data(
        method, test_utils.MULTIPLE_ITEM_COUNT)

    _, _, batch_to_item_id_dict = batch_creator.create_batch(
        batch_id, item_rows, method)

    self.assertEqual(test_utils.MULTIPLE_ITEM_COUNT, len(batch_to_item_id_dict))
    self.assertEqual('test id', batch_to_item_id_dict.get(0))
    self.assertEqual('test id', batch_to_item_id_dict.get(1))
