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

"""Integration tests for BigQuery Client."""

import os
import unittest

import bigquery_client

# Download service account key from GCP console and place it as a file which
# SERVICE_ACCOUNT_PATH points to.
# Please read the GCP help page to understand the way to download service
# account key:
# https://cloud.google.com/iam/docs/creating-managing-service-account-keys
SERVICE_ACCOUNT_PATH = './config/service_account.json'
DATASET_NAME = 'test_dataset'
TABLE_NAME = 'test_table'
PROJECT_ID = os.environ['PROJECT_ID']


class BigQueryClientTest(unittest.TestCase):

  def test_from_service_account_json_with_valid_credential_path(self):
    bq_client = bigquery_client.BigQueryClient.from_service_account_json(
        SERVICE_ACCOUNT_PATH, DATASET_NAME, TABLE_NAME)
    self.assertEqual(PROJECT_ID, bq_client._bigquery_client.project)

  def test_from_service_account_json_with_non_existing_credential_path(self):
    with self.assertRaises(IOError):
      bigquery_client.BigQueryClient.from_service_account_json(
          'wrong_path', DATASET_NAME, TABLE_NAME)
