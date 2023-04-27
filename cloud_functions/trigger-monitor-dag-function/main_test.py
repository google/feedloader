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

"""Unit tests for the Trigger DAG Cloud Function."""
import os
from unittest import mock

from absl.testing import parameterized

import main

_TEST_DAG_NAME = 'dag-name'
_TEST_WEBSERVER_ID = (
    'https://12345-dot-us-central1.composer.googleusercontent.com'
)


@mock.patch.dict(
    os.environ, {
        'DAG_NAME': _TEST_DAG_NAME,
        'WEBSERVER_ID': _TEST_WEBSERVER_ID,
    })
class TriggerMonitorDagFunctionTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.event = {
        'bucket': 'feed-bucket',
        'name': 'filename',
        'metageneration': 'test-metageneration',
        'timeCreated': '0',
        'updated': '0'
    }
    self.context = mock.create_autospec('google.cloud.functions.Context')
    self.context.event_id = '12345'
    self.context.event_type = 'gcs-event'
    self.context.timestamp = '2021-06-05T08:16:15.183Z'

  @mock.patch.object(
      main,
      '_make_composer_web_server_request',
      side_effect=Exception('Bad request: JSON body error'),
  )
  def test_json_body_error(self, _):
    trigger_event = None
    with self.assertRaises(Exception) as context:
      main.trigger_dag(trigger_event, self.context)

    self.assertIn('Bad request: JSON body error', str(context.exception))

  @mock.patch.object(main, 'AuthorizedSession')
  def test_http_forbidden_response_error(self, mock_authorized_session):
    trigger_event = {'file': 'some-gcs-file'}

    mock_authorized_session.return_value.request.return_value.status_code = 403
    with self.assertRaises(main.requests.HTTPError) as context:
      main.trigger_dag(trigger_event, self.context)

    self.assertIn(
        'You do not have permission to perform this operation. ',
        str(context.exception),
    )

  @mock.patch.object(main, '_make_composer_web_server_request', autospec=True)
  def test_api_endpoint(self, mock_make_composer_web_server_request):
    main.trigger_dag(self.event, self.context)

    mock_make_composer_web_server_request.assert_called_once_with(
        'https://12345-dot-us-central1.composer.googleusercontent.com/api/v1/dags/dag-name/dagRuns',
        method='POST',
        json={'conf': self.event},
    )
