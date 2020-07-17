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

"""Tests for Monitoring Completion Mailer Service."""

import unittest
from google.appengine.ext import testbed

import json
import main

_HTTP_OK = 200
_HTTP_UNAUTHORIZED = 401

DUMMY_SUCCESS_COUNT = 1
DUMMY_FAILURE_COUNT = 2
DUMMY_SKIPPED_COUNT = 3

OPERATION_UPSERT = 'upsert'
OPERATION_DELETE = 'delete'
OPERATION_PREVENT_EXPIRING = 'prevent_expiring'

KEY_OPERATION = 'operation'
KEY_SUCCESS_COUNT = 'success_count'
KEY_FAILURE_COUNT = 'failure_count'
KEY_SKIPPED_COUNT = 'skipped_count'


class MainTest(unittest.TestCase):

  def setUp(self):
    super(MainTest, self).setUp()
    main.app.testing = True
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    main.app.config['PUBSUB_VERIFICATION_TOKEN'] = 'testtoken'
    main.app.config['EMAIL_TO'] = 'testemailaddress'
    self.test_client = main.app.test_client()

  def tearDown(self):
    super(MainTest, self).tearDown()
    self.testbed.deactivate()

  def test_index(self):
    response = self.test_client.get('/health')
    self.assertEqual(_HTTP_OK, response.status_code)

  def test_pubsub_push_success(self):
    request_params = {'token': 'testtoken'}
    request_data = _create_pubsub_msg()
    response = self.test_client.post(
        '/pubsub/push', query_string=request_params, data=request_data)
    self.assertEqual(_HTTP_OK, response.status_code)

  def test_pubsub_push_success_when_pubsub_msg_empty(self):
    request_params = {'token': 'testtoken'}

    response = self.test_client.post(
        '/pubsub/push', query_string=request_params, data='{}')
    self.assertEqual(_HTTP_OK, response.status_code)

  def test_pubsub_push_failure(self):
    request_params = {'token': 'wrongtoken'}
    response = self.test_client.post(
        '/pubsub/push', query_string=request_params)
    self.assertEqual(_HTTP_UNAUTHORIZED, response.status_code)


def _create_pubsub_msg():
  # Create string interpolation dict for Python 2 string interpolation
  msg_dict = {
      'operation_upsert': OPERATION_UPSERT,
      'operation_delete': OPERATION_DELETE,
      'operation_prevent_expiring': OPERATION_PREVENT_EXPIRING,
      'operation': KEY_OPERATION,
      'success_count_key': KEY_SUCCESS_COUNT,
      'failure_count_key': KEY_FAILURE_COUNT,
      'skipped_count_key': KEY_SKIPPED_COUNT,
      'success_count': DUMMY_SUCCESS_COUNT,
      'failure_count': DUMMY_FAILURE_COUNT,
      'skipped_count': DUMMY_SKIPPED_COUNT
  }
  # Create PubSub message main body
  content_api_result_dict = (
      '{{"{operation_upsert}": {{"{operation}": "{operation_upsert}", '
      '"{success_count_key}": {success_count}, '
      '"{failure_count_key}": {failure_count}, '
      '"{skipped_count_key}": {skipped_count}}}, '
      '"{operation_delete}": {{"{operation}": "{operation_delete}", '
      '"{success_count_key}": {success_count}, '
      '"{failure_count_key}": {failure_count}, '
      '"{skipped_count_key}": {skipped_count}}}, '
      '"{operation_prevent_expiring}": {{"{operation}": '
      '"{operation_prevent_expiring}", "{success_count_key}": '
      '{success_count}, "{failure_count_key}": '
      '{failure_count}, "{skipped_count_key}": '
      '{skipped_count}}}}}').format(**msg_dict)
  # Wrap main body in PubSub message wrapper
  expected_publish_message = {
      'message': {
          'attributes': {
              'content_api_results': content_api_result_dict,
          }
      }
  }
  # Encode message as JSON string
  encoded_publish_message = json.dumps(expected_publish_message)
  return encoded_publish_message


if __name__ == '__main__':
  unittest.main()
