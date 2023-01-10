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

"""Tests for google3.corp.gtech.ads.shopping_feed_optimizer.get_cc_client_id."""
import io
import unittest
import unittest.mock as mock

import get_cc_client_id

_TEST_PROJECT_ID = 'testprojectid'
_TEST_LOCATION = 'testlocation'
_TEST_ENVIRONMENT = 'testenvironment'


class GetCloudComposerClientIdTest(unittest.TestCase):

  @mock.patch('sys.stdout', new_callable=io.StringIO)
  @mock.patch('google.auth')
  @mock.patch('requests.get')
  def test_get_client_id(self, mock_requests_get, mock_google_auth,
                         mock_stdout):
    mock_google_auth.default.return_value = ('creds', 12345)
    mock_requests_get.return_value.headers = {
        'location': 'http://www.google.com/unittest?client_id=12345'
    }
    get_cc_client_id.get_client_id(_TEST_PROJECT_ID, _TEST_LOCATION,
                                   _TEST_ENVIRONMENT)
    self.assertEqual(mock_stdout.getvalue(), '12345\n')
