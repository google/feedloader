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

"""Unit tests for models in models.py."""

import unittest

from models import upload_task

DUMMY_START_INDEX = 1
DUMMY_BATCH_SIZE = 2
DUMMY_TIMESTAMP = '00010101000000'
DUMMY_CHANNEL = 'local'


class UploadTaskTest(unittest.TestCase):

  def test_from_json_with_correct_data(self):
    dummy_json_data = {
        'start_index': DUMMY_START_INDEX,
        'batch_size': DUMMY_BATCH_SIZE,
        'timestamp': DUMMY_TIMESTAMP,
        'channel': DUMMY_CHANNEL,
    }
    task = upload_task.UploadTask.from_json(dummy_json_data)
    self.assertEqual(DUMMY_START_INDEX, task.start_index)
    self.assertEqual(DUMMY_BATCH_SIZE, task.batch_size)
    self.assertEqual(DUMMY_TIMESTAMP, task.timestamp)
    self.assertEqual(DUMMY_CHANNEL, task.channel)
