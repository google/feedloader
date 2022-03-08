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

"""Unit tests for tasks_client."""

import unittest
import unittest.mock

from google.cloud.tasks import types

import tasks_client

# Download service account key from GCP console and place it as a file which
# SERVICE_ACCOUNT_PATH points to.
# Please read the GCP help page to understand the way to download service
# account key:
# https://cloud.google.com/iam/docs/creating-managing-service-account-keys
SERVICE_ACCOUNT_PATH = './config/gcp_service_account.json'
PROJECT_ID = 'dummy-project'
LOCATION = 'asia-northeast1'
QUEUE_NAME = 'updated-item'


class TasksClientTest(unittest.TestCase):

  def setUp(self):
    super(TasksClientTest, self).setUp()
    self.mock_client = unittest.mock.Mock()
    self.tasks_client = tasks_client.TasksClient(self.mock_client)

  def test_is_queue_empty_when_empty(self):
    self.mock_client.list_tasks.return_value = []
    self.assertTrue(
        self.tasks_client.is_queue_empty(PROJECT_ID, LOCATION, QUEUE_NAME))

  def test_is_queue_empty_when_not_empty(self):
    self.mock_client.list_tasks.return_value = [types.Task()]
    self.assertFalse(
        self.tasks_client.is_queue_empty(PROJECT_ID, LOCATION, QUEUE_NAME))
