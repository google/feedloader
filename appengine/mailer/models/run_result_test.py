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

"""Tests for run_result.py."""

import unittest

import run_result

OPERATION_UPSERT = 'upsert'

DUMMY_SUCCESS_COUNT = 1
DUMMY_FAILURE_COUNT = 2
DUMMY_SKIPPED_COUNT = 3


class RunResultTest(unittest.TestCase):

  def test_from_dict(self):
    input_dict = {
        'operation': OPERATION_UPSERT,
        'success_count': DUMMY_SUCCESS_COUNT,
        'failure_count': DUMMY_FAILURE_COUNT,
        'skipped_count': DUMMY_SKIPPED_COUNT
    }
    result = run_result.RunResult.from_dict(input_dict)
    self.assertEqual(OPERATION_UPSERT, result.operation)
    self.assertEqual(DUMMY_SUCCESS_COUNT, result.success_count)
    self.assertEqual(DUMMY_FAILURE_COUNT, result.failure_count)
    self.assertEqual(DUMMY_SKIPPED_COUNT, result.skipped_count)

  def test_get_total_count(self):
    input_dict = {
        'operation': OPERATION_UPSERT,
        'success_count': DUMMY_SUCCESS_COUNT,
        'failure_count': DUMMY_FAILURE_COUNT,
        'skipped_count': DUMMY_SKIPPED_COUNT
    }
    result = run_result.RunResult.from_dict(input_dict)
    self.assertEqual(6, result.get_total_count())
