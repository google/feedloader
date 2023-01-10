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

"""Tests for App Engine server of uploader service."""

import http
import json
import os
import socket
import unittest.mock as mock

from absl.testing import parameterized
from google.cloud import bigquery
from googleapiclient import errors

import constants
import main
from models import failure
from models import process_result

DUMMY_CHANNEL = constants.Channel.ONLINE
DUMMY_OPERATION = constants.Operation.UPSERT
DUMMY_IS_MCA = 'False'
DUMMY_MERCHANT_ID = '12345'
DUMMY_SHOPTIMIZER_API_INTEGRATION_ON = 'False'
DUMMY_ROWS = [bigquery.Row(['0001'], {'item_id': 0})]
DUMMY_START_INDEX = 0
DUMMY_BATCH_SIZE = 1000
DUMMY_TIMESTAMP = '0001-01-01:00:00:00'
DUMMY_REQUEST_BODY = json.dumps({
    'start_index': DUMMY_START_INDEX,
    'batch_size': DUMMY_BATCH_SIZE,
    'timestamp': DUMMY_TIMESTAMP,
    'channel': DUMMY_CHANNEL.value,
})
DUMMY_SUCCESSES = ['0001', '0002', '0003']
DUMMY_FAILURES = [failure.Failure('0004', 'Error message')]
DUMMY_SKIPPED = ['0005']
INSERT_URL = '/insert_items'
DELETE_URL = '/delete_items'
PREVENT_EXPIRING_URL = '/prevent_expiring_items'
REQUEST_HEADERS = {'X-AppEngine-TaskExecutionCount': '0'}


@mock.patch.dict(
    os.environ, {
        'IS_MCA': DUMMY_IS_MCA,
        'MERCHANT_ID': DUMMY_MERCHANT_ID,
        'SHOPTIMIZER_API_INTEGRATION_ON': DUMMY_SHOPTIMIZER_API_INTEGRATION_ON,
    })
class MainTest(parameterized.TestCase):

  def setUp(self):
    super(MainTest, self).setUp()
    main.app.testing = True
    self.test_client = main.app.test_client()

    self.mock_bq_client = mock.patch(
        'bigquery_client.BigQueryClient', autospec=True).start()
    self.mock_content_api_client = mock.patch(
        'content_api_client.ContentApiClient', autospec=True).start()
    self.mock_recorder = mock.patch(
        'result_recorder.ResultRecorder', autospec=True).start()
    self.mock_shoptimizer_client = mock.patch(
        'shoptimizer_client.ShoptimizerClient', autospec=True).start()

    self.mock_bq_client.from_service_account_json.return_value.load_items.return_value = DUMMY_ROWS
    self.mock_content_api_client.return_value.process_items.return_value = (
        DUMMY_SUCCESSES, DUMMY_FAILURES)
    mock_cloud_logging = mock.patch('main.cloud_logging')
    mock_cloud_logging.start()
    self.addCleanup(mock.patch.stopall)

  @parameterized.named_parameters(
      {
          'testcase_name': 'insert',
          'url': INSERT_URL
      },
      {
          'testcase_name': 'delete',
          'url': DELETE_URL
      },
  )
  def test_run_process_should_return_ok_when_batch_size_is_positive(self, url):
    response = self.test_client.post(
        url, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  def test_run_process_should_do_nothing_when_batch_size_is_zero(self):
    request_body = json.dumps({
        'start_index': DUMMY_START_INDEX,
        'batch_size': 0,
        'timestamp': DUMMY_TIMESTAMP,
        'channel': DUMMY_CHANNEL.value,
    })

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=REQUEST_HEADERS)

    self.mock_bq_client.from_service_account_json.return_value.load_items.assert_not_called(
    )
    self.mock_content_api_client.return_value.process_items.assert_not_called()
    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_not_called(
    )
    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  def test_run_process_should_return_error_when_channel_is_invalid(self):
    request_body = json.dumps({
        'start_index': DUMMY_START_INDEX,
        'batch_size': DUMMY_BATCH_SIZE,
        'timestamp': DUMMY_TIMESTAMP,
        'channel': 'invalid_channel',
    })
    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=REQUEST_HEADERS)
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)
    self.assertEqual(b'Invalid channel', response.data)

  def test_run_process_should_load_items_from_biqquery(self):
    self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_bq_client.from_service_account_json.return_value.load_items.assert_called_once(
    )

  def test_run_process_should_return_error_when_failing_to_load_items_from_bigquery(
      self):
    self.mock_bq_client.from_service_account_json.return_value.load_items.side_effect = errors.HttpError(
        mock.MagicMock(), b'')

    response = self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_run_process_should_call_content_api(self):
    self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_content_api_client.return_value.process_items.assert_called_once()

  def test_run_process_should_call_content_api_with_insert_when_operation_is_insert(
      self):
    self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY, mock.ANY, mock.ANY, constants.Method.INSERT, DUMMY_CHANNEL)

  def test_run_process_should_call_content_api_with_insert_when_operation_is_prevent_expiring(
      self):
    self.test_client.post(
        PREVENT_EXPIRING_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY, mock.ANY, mock.ANY, constants.Method.INSERT, DUMMY_CHANNEL)

  def test_run_process_should_call_content_api_with_delete_when_operation_is_delete(
      self):
    self.test_client.post(
        DELETE_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY, mock.ANY, mock.ANY, constants.Method.DELETE, DUMMY_CHANNEL)

  @parameterized.named_parameters(
      {
          'testcase_name': 'bad_request',
          'reason': 'BAD REQUEST',
          'status': http.HTTPStatus.BAD_REQUEST
      }, {
          'testcase_name': 'internal_server_error',
          'reason': 'INTERNAL SERVER ERROR',
          'status': http.HTTPStatus.INTERNAL_SERVER_ERROR
      })
  def test_run_process_should_return_the_same_error_when_content_api_call_returns_error_and_retry_is_suggested(
      self, reason, status):
    with mock.patch('content_api_client.suggest_retry') as suggest_retry:
      suggest_retry.return_value = True
      self.mock_content_api_client.return_value.process_items.side_effect = (
          errors.HttpError(mock.MagicMock(status=status, reason=reason), b''))

      response = self.test_client.post(
          INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

      self.assertEqual(status, response.status_code)
      self.assertEqual(reason, response.data.decode())

  def test_run_process_should_return_error_when_content_api_call_returns_error_and_retry_is_not_suggested(
      self):
    with mock.patch('content_api_client.suggest_retry') as suggest_retry:
      suggest_retry.return_value = False
      self.mock_content_api_client.return_value.process_items.side_effect = (
          errors.HttpError(
              mock.MagicMock(
                  status=http.HTTPStatus.PAYMENT_REQUIRED,
                  reason='Payment Required'), b''))

      response = self.test_client.post(
          INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

      self.assertEqual(http.HTTPStatus.PAYMENT_REQUIRED, response.status_code)

  def test_run_process_should_return_timeout_error_when_content_api_call_returns_socket_timeout_error(
      self):
    self.mock_content_api_client.return_value.process_items.side_effect = (
        socket.timeout())

    response = self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.assertEqual(http.HTTPStatus.REQUEST_TIMEOUT, response.status_code)

  def test_run_process_should_log_error_when_max_retry_attempts_exhausted(self):
    max_retry_count = 5
    http_error = errors.HttpError(
        mock.MagicMock(
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            reason='Server got itself in trouble'), b'')
    self.mock_content_api_client.return_value.process_items.side_effect = (
        http_error)

    with self.assertLogs(level='ERROR') as log:
      self.test_client.post(
          INSERT_URL,
          data=DUMMY_REQUEST_BODY,
          headers={'X-AppEngine-TaskExecutionCount': f'{max_retry_count}'})

      self.assertIn(
          'ERROR:root:Batch #1 with operation upsert, initiation timestamp '
          f'{DUMMY_TIMESTAMP}, and channel {DUMMY_CHANNEL.value} '
          f'failed and will not be retried. '
          f'Error: {http_error}', log.output)

  def test_run_process_should_return_ok_when_execution_count_header_missing_and_content_api_call_returns_success(
      self):
    self.mock_bq_client.from_service_account_json.return_value.load_items.return_value = DUMMY_ROWS
    response = self.test_client.post(INSERT_URL, data=DUMMY_REQUEST_BODY)
    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  def test_run_process_should_log_error_when_execution_count_header_missing_and_content_api_call_returns_error(
      self):
    http_error = errors.HttpError(
        mock.MagicMock(
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            reason='Server got itself in trouble'), b'')
    self.mock_content_api_client.return_value.process_items.side_effect = (
        http_error)

    with self.assertLogs(level='ERROR') as log:
      self.test_client.post(INSERT_URL, data=DUMMY_REQUEST_BODY)

      self.assertIn(
          'ERROR:root:Batch #1 with operation upsert, initiation timestamp '
          f'{DUMMY_TIMESTAMP}, and channel {DUMMY_CHANNEL.value} '
          f'failed and will not be retried. '
          f'Error: {http_error}', log.output)

  @parameterized.named_parameters(
      {
          'testcase_name': 'channel_is_online',
          'channel': constants.Channel.ONLINE
      },
      {
          'testcase_name': 'channel_is_local',
          'channel': constants.Channel.LOCAL
      },
  )
  def test_run_process_should_record_result_when_content_api_call_returns_ok(
      self, channel: constants.Channel):
    expected_batch_id = int(DUMMY_START_INDEX / DUMMY_BATCH_SIZE) + 1
    expected_result = process_result.ProcessResult(DUMMY_SUCCESSES,
                                                   DUMMY_FAILURES, [])

    request_body = json.dumps({
        'start_index': DUMMY_START_INDEX,
        'batch_size': DUMMY_BATCH_SIZE,
        'timestamp': DUMMY_TIMESTAMP,
        'channel': channel.value,
    })

    self.test_client.post(
        INSERT_URL, data=request_body, headers=REQUEST_HEADERS)

    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_called_once_with(
        channel,
        DUMMY_OPERATION,
        expected_result,
        DUMMY_TIMESTAMP,
        expected_batch_id,
    )

  def test_run_process_should_record_that_all_items_failed_when_content_api_call_returns_error(
      self):
    dummy_http_error = errors.HttpError(
        mock.MagicMock(
            status=http.HTTPStatus.BAD_REQUEST, reason='Bad Request'), b'')
    self.mock_content_api_client.return_value.process_items.side_effect = (
        dummy_http_error)
    dummy_failures = [
        failure.Failure(
            str(item.get('item_id', 'Missing ID')),
            dummy_http_error.resp.reason) for item in DUMMY_ROWS
    ]
    expected_result = process_result.ProcessResult([], dummy_failures, [])
    expected_batch_id = int(DUMMY_START_INDEX / DUMMY_BATCH_SIZE) + 1
    self.mock_bq_client.from_service_account_json.return_value.load_items.return_value = DUMMY_ROWS

    self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_called_once_with(
        DUMMY_CHANNEL,
        DUMMY_OPERATION,
        expected_result,
        DUMMY_TIMESTAMP,
        expected_batch_id,
    )

  @mock.patch.dict(os.environ, {
      'SHOPTIMIZER_API_INTEGRATION_ON': 'True',
  })
  def test_run_process_should_call_shoptimizer_when_operation_is_insert(self):
    self.test_client.post(
        INSERT_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_shoptimizer_client.return_value.shoptimize.assert_called_once()

  @mock.patch.dict(os.environ, {
      'SHOPTIMIZER_API_INTEGRATION_ON': 'True',
  })
  def test_run_process_should_call_shoptimizer_when_operation_is_prevent_expiring(
      self):
    self.test_client.post(
        PREVENT_EXPIRING_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_shoptimizer_client.return_value.shoptimize.assert_called_once()

  def test_run_process_should_call_shoptimizer_when_operation_is_delete(self):
    self.test_client.post(
        DELETE_URL, data=DUMMY_REQUEST_BODY, headers=REQUEST_HEADERS)

    self.mock_shoptimizer_client.return_value.shoptimize.assert_not_called()
