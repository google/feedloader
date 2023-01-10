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

"""Integration tests for result_recorder.py."""

import unittest

import result_recorder
import utils

# Download service account key from GCP console and place it as a file which
# SERVICE_ACCOUNT_PATH points to.
# Please read the GCP help page to understand the way to download service
# account key:
# https://cloud.google.com/iam/docs/creating-managing-service-account-keys
_SERVICE_ACCOUNT_PATH = './config/gcp_service_account.json'
_WRONG_SERVICE_ACCOUNT_PATH = './wrong_path/gcp_service_account.json'
_DATASET_NAME = 'test_dataset'
_TABLE_COUNTS_NAME = 'test_table_counts'
_TABLE_ITEMS_NAME = 'test_table_items'
_PROJECT_ID = utils.load_environment_variable('PROJECT_ID')


class ResultRecorderIntegrationTest(unittest.TestCase):

  def test_from_service_account_json_with_valid_credential_path(self):
    recorder = result_recorder.ResultRecorder.from_service_account_json(
        _SERVICE_ACCOUNT_PATH, _DATASET_NAME, _TABLE_COUNTS_NAME,
        _TABLE_ITEMS_NAME)
    self.assertEqual(_PROJECT_ID, recorder._client.project)

  def test_from_service_account_json_with_non_existing_credential_path(self):
    with self.assertRaises(IOError):
      result_recorder.ResultRecorder.from_service_account_json(
          _WRONG_SERVICE_ACCOUNT_PATH, _DATASET_NAME, _TABLE_COUNTS_NAME,
          _TABLE_ITEMS_NAME)
