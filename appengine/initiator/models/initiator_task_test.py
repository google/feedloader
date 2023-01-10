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

"""Unit tests for models in models.py."""

import unittest

from models import initiator_task

DUMMY_DELETE_COUNT = 1
DUMMY_EXPIRING_COUNT = 2
DUMMY_UPSERT_COUNT = 3


class InitiatorTaskTest(unittest.TestCase):

  def test_from_json_with_valid_dict(self):
    dummy_json_data = {
        'deleteCount': DUMMY_DELETE_COUNT,
        'expiringCount': DUMMY_EXPIRING_COUNT,
        'upsertCount': DUMMY_UPSERT_COUNT
    }
    task = initiator_task.InitiatorTask.from_json(dummy_json_data)
    self.assertEqual(int(DUMMY_UPSERT_COUNT), task.upsert_count)
    self.assertEqual(int(DUMMY_DELETE_COUNT), task.delete_count)
    self.assertEqual(int(DUMMY_EXPIRING_COUNT), task.expiring_count)

  def test_init_with_invalid_num_rows(self):
    invalid_json_data = {
        'deleteCount': 'not a number',
        'expiringCount': 'not a number',
        'upsertCount': 'not a number'
    }
    with self.assertRaises(ValueError):
      initiator_task.InitiatorTask.from_json(invalid_json_data)
