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

"""Tests for the Build Reporter Service."""

import httplib
import unittest
from google.appengine.ext import testbed

import main
from test_data import pubsub_msgs


class MainTest(unittest.TestCase):

  def setUp(self):
    super(MainTest, self).setUp()
    main.app.testing = True
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_mail_stub()
    self.mail_stub = self.testbed.get_stub(testbed.MAIL_SERVICE_NAME)
    self.test_client = main.app.test_client()

  def tearDown(self):
    super(MainTest, self).tearDown()
    self.testbed.deactivate()

  def test_health(self):
    response = self.test_client.get('/health')
    self.assertEqual(httplib.OK, response.status_code)

  def test_pubsub_push_success(self):
    request_data = pubsub_msgs.STATUS_SUCCESS_TAG_SHOPTIMIZER
    response = self.test_client.post('pubsub/push', data=request_data)
    sent_messages = self.mail_stub.get_sent_messages()
    self.assertEqual(1, len(sent_messages))
    self.assertEqual(httplib.OK, response.status_code)

  def test_email_not_sent_when_status_not_in_statuses_to_report(self):
    request_data = pubsub_msgs.STATUS_WAITING_TAG_SHOPTIMIZER
    response = self.test_client.post('pubsub/push', data=request_data)
    sent_messages = self.mail_stub.get_sent_messages()
    self.assertEqual(0, len(sent_messages))
    self.assertEqual(httplib.OK, response.status_code)

  def test_email_not_sent_when_build_tag_not_in_tags_to_report(self):
    request_data = pubsub_msgs.STATUS_SUCCESS_TAG_UNDEFINED
    response = self.test_client.post('pubsub/push', data=request_data)
    sent_messages = self.mail_stub.get_sent_messages()
    self.assertEqual(0, len(sent_messages))
    self.assertEqual(httplib.OK, response.status_code)


if __name__ == '__main__':
  unittest.main()
