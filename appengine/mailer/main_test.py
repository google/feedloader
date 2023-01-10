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

"""Tests for Monitoring Completion Mailer Service."""

import unittest
from google.appengine.ext import testbed

from absl.testing import parameterized
import json
import main
import mock

_HTTP_OK = 200
_HTTP_UNAUTHORIZED = 401

_KEY_CHANNEL = 'channel'
_CHANNEL_ONLINE = 'online'
_CHANNEL_LOCAL = 'local'

_KEY_OPERATION = 'operation'
_OPERATION_UPSERT = 'upsert'
_OPERATION_DELETE = 'delete'
_OPERATION_PREVENT_EXPIRING = 'prevent_expiring'

_KEY_SUCCESS_COUNT = 'success_count'
_KEY_FAILURE_COUNT = 'failure_count'
_KEY_SKIPPED_COUNT = 'skipped_count'
_DUMMY_SUCCESS_COUNT = 1
_DUMMY_FAILURE_COUNT = 2
_DUMMY_SKIPPED_COUNT = 3

_DUMMY_PUBSUB_TOKEN = 'testtoken'


class MainTest(parameterized.TestCase):

  def setUp(self):
    super(MainTest, self).setUp()
    main.app.testing = True
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    self.testbed.setup_env(PUBSUB_VERIFICATION_TOKEN=_DUMMY_PUBSUB_TOKEN)
    self.testbed.setup_env(EMAIL_TO='testemailaddress')
    self.testbed.setup_env(USE_LOCAL_INVENTORY_ADS='False')
    mock.patch('main._project_id').start()
    main._project_id.return_value = 'test-project-id'
    self.test_client = main.app.test_client()
    self.addCleanup(mock.patch.stopall)

  def tearDown(self):
    super(MainTest, self).tearDown()
    self.testbed.deactivate()

  def test_index(self):
    response = self.test_client.get('/health')
    self.assertEqual(_HTTP_OK, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'online_and_local',
          'channels': (_CHANNEL_ONLINE, _CHANNEL_LOCAL)
      }, {
          'testcase_name': 'online_only',
          'channels': (_CHANNEL_ONLINE,)
      })
  def test_pubsub_push_success(self, channels):
    request_params = {'token': _DUMMY_PUBSUB_TOKEN}
    request_data = _create_pubsub_msg(channels)
    response = self.test_client.post(
        '/pubsub/push', query_string=request_params, data=request_data)
    self.assertEqual(_HTTP_OK, response.status_code)

  def test_pubsub_push_success_when_pubsub_msg_empty(self):
    request_params = {'token': 'testtoken'}

    response = self.test_client.post(
        '/pubsub/push', query_string=request_params, data='{}')
    self.assertEqual(_HTTP_OK, response.status_code)

  @mock.patch('google.appengine.api.mail.EmailMessage')
  def test_local_results_are_added_to_email_content_when_local_inventory_is_enabled(
      self, email_message):
    self.testbed.setup_env(USE_LOCAL_INVENTORY_ADS='True', overwrite=True)
    request_params = {'token': _DUMMY_PUBSUB_TOKEN}
    channels = (_CHANNEL_ONLINE, _CHANNEL_LOCAL)
    request_data = _create_pubsub_msg(channels)
    self.test_client.post(
        '/pubsub/push', query_string=request_params, data=request_data)
    email_content = email_message.call_args.kwargs['html']
    self.assertIn('Run Results for Local Inventory:', email_content)

  @mock.patch('google.appengine.api.mail.EmailMessage')
  def test_local_results_are_not_added_to_email_content_when_local_inventory_is_disabled(
      self, email_message):
    self.testbed.setup_env(USE_LOCAL_INVENTORY_ADS='False', overwrite=True)
    request_params = {'token': _DUMMY_PUBSUB_TOKEN}
    channels = (_CHANNEL_ONLINE,)
    request_data = _create_pubsub_msg(channels)
    self.test_client.post(
        '/pubsub/push', query_string=request_params, data=request_data)
    email_content = email_message.call_args.kwargs['html']
    self.assertNotIn('Run Results for Local Inventory:', email_content)

  def test_pubsub_push_failure(self):
    request_params = {'token': 'wrongtoken'}
    response = self.test_client.post(
        '/pubsub/push', query_string=request_params)
    self.assertEqual(_HTTP_UNAUTHORIZED, response.status_code)


def _create_pubsub_msg(channels=(_CHANNEL_ONLINE,)):
  # Create PubSub message main body
  content_api_result_in_list = []
  for channel in channels:
    for operation in (_OPERATION_UPSERT, _OPERATION_DELETE,
                      _OPERATION_PREVENT_EXPIRING):
      content_api_result_in_list.append({
          _KEY_CHANNEL: channel,
          _KEY_OPERATION: operation,
          _KEY_SUCCESS_COUNT: _DUMMY_SUCCESS_COUNT,
          _KEY_FAILURE_COUNT: _DUMMY_FAILURE_COUNT,
          _KEY_SKIPPED_COUNT: _DUMMY_SKIPPED_COUNT
      })
  content_api_result_in_string = json.dumps(content_api_result_in_list)
  # Wrap main body in PubSub message wrapper
  expected_publish_message = {
      'message': {
          'attributes': {
              'content_api_results': content_api_result_in_string,
          }
      }
  }
  # Encode message as JSON string
  encoded_publish_message = json.dumps(expected_publish_message)
  return encoded_publish_message


if __name__ == '__main__':
  unittest.main()
