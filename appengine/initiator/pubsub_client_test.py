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

"""Tests for pubsub_client.py."""
import json
import unittest
import unittest.mock as mock

from models import operation_counts
import pubsub_client

DUMMY_PROJECT_ID = 'dummy-project'
DUMMY_TOPIC_NAME = 'dummy-topic'

DUMMY_INSERT_SUCCESS_COUNT = 1
DUMMY_INSERT_FAILURE_COUNT = 2
DUMMY_INSERT_SKIPPED_COUNT = 3
DUMMY_DELETE_SUCCESS_COUNT = 4
DUMMY_DELETE_FAILURE_COUNT = 5
DUMMY_DELETE_SKIPPED_COUNT = 6
DUMMY_EXPIRATION_SUCCESS_COUNT = 7
DUMMY_EXPIRATION_FAILURE_COUNT = 8
DUMMY_EXPIRATION_SKIPPED_COUNT = 9

OPERATION_UPSERT = 'upsert'
OPERATION_DELETE = 'delete'
OPERATION_EXPIRATION = 'expiration'

KEY_OPERATION = 'operation'
KEY_SUCCESS_COUNT = 'success_count'
KEY_FAILURE_COUNT = 'failure_count'
KEY_SKIPPED_COUNT = 'skipped_count'


class PubsubClientTest(unittest.TestCase):

  def setUp(self) -> None:
    super(PubsubClientTest, self).setUp()
    self.mock_client = mock.MagicMock()
    self.pubsub_client = pubsub_client.PubSubClient(self.mock_client)

  def test_publisher_pushes_message_to_topic(self):
    dummy_operation_counts = {
        OPERATION_UPSERT:
            operation_counts.OperationCounts(OPERATION_UPSERT,
                                             DUMMY_INSERT_SUCCESS_COUNT,
                                             DUMMY_INSERT_FAILURE_COUNT,
                                             DUMMY_INSERT_SKIPPED_COUNT),
        OPERATION_DELETE:
            operation_counts.OperationCounts(OPERATION_DELETE,
                                             DUMMY_DELETE_SUCCESS_COUNT,
                                             DUMMY_DELETE_FAILURE_COUNT,
                                             DUMMY_DELETE_SKIPPED_COUNT),
        OPERATION_EXPIRATION:
            operation_counts.OperationCounts(OPERATION_EXPIRATION,
                                             DUMMY_EXPIRATION_SUCCESS_COUNT,
                                             DUMMY_EXPIRATION_FAILURE_COUNT,
                                             DUMMY_EXPIRATION_SKIPPED_COUNT)
    }

    self.pubsub_client.trigger_result_email(DUMMY_PROJECT_ID, DUMMY_TOPIC_NAME,
                                            dummy_operation_counts)

    expected_topic = f'projects/{DUMMY_PROJECT_ID}/topics/{DUMMY_TOPIC_NAME}'
    expected_operation_count_str = (
        f'{{"{OPERATION_UPSERT}": {{"{KEY_OPERATION}": "{OPERATION_UPSERT}", '
        f'"{KEY_SUCCESS_COUNT}": {DUMMY_INSERT_SUCCESS_COUNT}, '
        f'"{KEY_FAILURE_COUNT}": {DUMMY_INSERT_FAILURE_COUNT}, '
        f'"{KEY_SKIPPED_COUNT}": {DUMMY_INSERT_SKIPPED_COUNT}}}, '
        f'"{OPERATION_DELETE}": {{"{KEY_OPERATION}": "{OPERATION_DELETE}", '
        f'"{KEY_SUCCESS_COUNT}": {DUMMY_DELETE_SUCCESS_COUNT}, '
        f'"{KEY_FAILURE_COUNT}": {DUMMY_DELETE_FAILURE_COUNT}, '
        f'"{KEY_SKIPPED_COUNT}": {DUMMY_DELETE_SKIPPED_COUNT}}}, '
        f'"{OPERATION_EXPIRATION}": {{"{KEY_OPERATION}": '
        f'"{OPERATION_EXPIRATION}", "{KEY_SUCCESS_COUNT}": '
        f'{DUMMY_EXPIRATION_SUCCESS_COUNT}, "{KEY_FAILURE_COUNT}": '
        f'{DUMMY_EXPIRATION_FAILURE_COUNT}, "{KEY_SKIPPED_COUNT}": '
        f'{DUMMY_EXPIRATION_SKIPPED_COUNT}}}}}')
    expected_message = {
        'attributes': {
            'content_api_results': expected_operation_count_str,
        }
    }
    encoded_expected_message = json.dumps(expected_message).encode('utf-8')
    self.mock_client.publish.assert_called_with(expected_topic,
                                                encoded_expected_message)


if __name__ == '__main__':
  unittest.main()
