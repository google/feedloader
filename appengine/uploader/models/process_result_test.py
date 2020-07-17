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
"""Unit tests for process_result.py."""

import unittest

from models import failure
from models import process_result


class ProcessResultTest(unittest.TestCase):

  def test_add_success(self):
    result = process_result.ProcessResult([], [], [])
    result.add_successfully_processed_item_id('0001')

    self.assertEqual(1, result.get_success_count())
    self.assertEqual('0001', result.successfully_processed_item_ids[-1])

  def test_add_failure(self):
    result = process_result.ProcessResult([], [], [])
    result.add_content_api_failure(failure.Failure('0001', 'Error msg'))

    self.assertEqual(1, result.get_failure_count())
    self.assertEqual('0001', result.content_api_failures[-1].item_id)
    self.assertEqual('Error msg', result.content_api_failures[-1].error_msg)

  def test_add_skipped(self):
    result = process_result.ProcessResult([], [], [])
    result.add_skipped_item_id('0001')

    self.assertEqual(1, result.get_skipped_count())
    self.assertEqual('0001', result.skipped_item_ids[-1])

  def test_counts(self):
    result = process_result.ProcessResult(
        ['0001', '0002', '0003'], [failure.Failure('0004', 'Error message')],
        ['0005', '0006'])

    self.assertEqual(3, result.get_success_count())
    self.assertEqual(1, result.get_failure_count())
    self.assertEqual(2, result.get_skipped_count())

  def test_get_counts_str(self):
    result = process_result.ProcessResult(
        ['0001', '0002', '0003'], [failure.Failure('0004', 'Error message')],
        ['0005', '0006'])

    count_str = result.get_counts_str()

    self.assertEqual('Success: 3, Failure: 1, Skipped: 2.', count_str)

  def test_get_ids_str(self):
    result = process_result.ProcessResult(
        ['0001', '0002', '0003'], [failure.Failure('0004', 'Error message')],
        ['0005', '0006'])

    ids_str = result.get_ids_str()

    self.assertEqual(
        'Success: 0001, 0002, 0003, Failure: [\'ID: 0004, Error: Error message, \'], Skipped: 0005, 0006.',
        ids_str)
