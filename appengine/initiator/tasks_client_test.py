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

"""Tests for Task Queue Client."""

import unittest
import unittest.mock

import tasks_client

TARGET_URL = '/dummy'
PROJECT_ID = 'dummy-project'
LOCATION = 'dummy-location'
QUEUE_NAME = 'processing-items'


class TasksClientTest(unittest.TestCase):

  def setUp(self):
    super(TasksClientTest, self).setUp()
    self.mock_client = unittest.mock.Mock()
    self.ct_client = tasks_client.TasksClient(self.mock_client, TARGET_URL,
                                              PROJECT_ID, LOCATION, QUEUE_NAME)

  def test_push_tasks(self):
    total_items = 2000
    batch_size = 1000
    timestamp = '20180101203010'
    channel = 'online'
    self.ct_client.push_tasks(total_items, batch_size, timestamp, channel)
    self.assertEqual(2, self.mock_client.create_task.call_count)
