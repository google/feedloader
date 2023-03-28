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
from unittest import mock

from absl.testing import parameterized
from google.cloud import bigquery
from googleapiclient import errors

import constants
import main
from models import failure
from models import process_result

DUMMY_OPERATION = constants.Operation.UPSERT
DUMMY_IS_MCA = 'False'
DUMMY_MERCHANT_ID = '12345'
DUMMY_SHOPTIMIZER_API_INTEGRATION_ON = 'False'
DUMMY_ROWS = [bigquery.Row(['0001'], {'item_id': 0})]
DUMMY_START_INDEX = 0
DUMMY_BATCH_SIZE = 1000
DUMMY_TIMESTAMP = '0001-01-01:00:00:00'
DUMMY_SUCCESSES = ['0001', '0002', '0003']
DUMMY_FAILURES = [failure.Failure('0004', 'Error message')]
DUMMY_SKIPPED = ['0005']
INSERT_URL = '/insert_items'
DELETE_URL = '/delete_items'
PREVENT_EXPIRING_URL = '/prevent_expiring_items'
HEADER_NAME_FOR_TASK_EXECUTION_COUNT = 'X-AppEngine-TaskExecutionCount'
TASK_QUEUE_NAME = 'processing-items'
TASK_QUEUE_NAME_LOCAL = TASK_QUEUE_NAME + main.LOCAL_SUFFIX_FOR_TASK_QUEUE
REQUEST_HEADERS = {
    HEADER_NAME_FOR_TASK_EXECUTION_COUNT: '0',
    main.HEADER_NAME_FOR_TASK_QUEUE: TASK_QUEUE_NAME,
}
REQUEST_HEADERS_LOCAL = {
    HEADER_NAME_FOR_TASK_EXECUTION_COUNT: '0',
    main.HEADER_NAME_FOR_TASK_QUEUE: TASK_QUEUE_NAME_LOCAL,
}
REQUEST_BODY_ONLINE = json.dumps({
    'start_index': DUMMY_START_INDEX,
    'batch_size': DUMMY_BATCH_SIZE,
    'timestamp': DUMMY_TIMESTAMP,
    'channel': constants.Channel.ONLINE.value,
})
REQUEST_BODY_LOCAL = json.dumps({
    'start_index': DUMMY_START_INDEX,
    'batch_size': DUMMY_BATCH_SIZE,
    'timestamp': DUMMY_TIMESTAMP,
    'channel': constants.Channel.LOCAL.value,
})


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

    from_service_account_json = (
        self.mock_bq_client.from_service_account_json.return_value
    )
    from_service_account_json.load_items.return_value = DUMMY_ROWS
    self.mock_content_api_client.return_value.process_items.return_value = (
        DUMMY_SUCCESSES, DUMMY_FAILURES)
    mock_cloud_logging = mock.patch('main.cloud_logging')
    mock_cloud_logging.start()
    self.addCleanup(mock.patch.stopall)

  @parameterized.named_parameters(
      {
          'testcase_name': 'insert',
          'url': INSERT_URL,
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'delete',
          'url': DELETE_URL,
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'insert_local',
          'url': INSERT_URL,
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_ok_when_batch_size_is_positive(
      self, url, request_headers, request_body
  ):
    response = self.test_client.post(
        url, data=request_body, headers=request_headers
    )

    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_do_nothing_when_batch_size_is_zero(
      self, request_headers, request_body
  ):
    request_body_dict = json.loads(request_body)
    request_body_dict['batch_size'] = 0
    request_body = json.dumps(request_body_dict)

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_bq_client.from_service_account_json.return_value.load_items.assert_not_called(
    )
    self.mock_content_api_client.return_value.process_items.assert_not_called()
    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_not_called(
    )
    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_error_when_channel_is_invalid(
      self, request_headers, request_body
  ):
    request_body_dict = json.loads(request_body)
    request_body_dict['channel'] = 'invalid_channel'
    request_body = json.dumps(request_body_dict)

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)
    self.assertEqual(b'Invalid channel', response.data)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_load_items_from_biqquery(
      self, request_headers, request_body
  ):
    self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_bq_client.from_service_account_json.return_value.load_items.assert_called_once(
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_error_when_failing_to_load_items_from_bigquery(
      self, request_headers, request_body
  ):
    self.mock_bq_client.from_service_account_json.return_value.load_items.side_effect = errors.HttpError(
        mock.MagicMock(), b'')

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_call_content_api(
      self, request_headers, request_body
  ):
    self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_content_api_client.return_value.process_items.assert_called_once()

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'channel': constants.Channel.ONLINE,
          'feed_type': constants.FeedType.PRIMARY,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'channel': constants.Channel.LOCAL,
          'feed_type': constants.FeedType.LOCAL,
      },
  )
  def test_run_process_should_call_content_api_with_insert_when_operation_is_insert(
      self, request_headers, request_body, channel, feed_type
  ):
    self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY,
        mock.ANY,
        mock.ANY,
        constants.Method.INSERT,
        channel,
        feed_type,
    )

  def test_run_process_should_call_content_api_with_insert_when_operation_is_prevent_expiring(
      self):
    self.test_client.post(
        PREVENT_EXPIRING_URL, data=REQUEST_BODY_ONLINE, headers=REQUEST_HEADERS
    )

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY,
        mock.ANY,
        mock.ANY,
        constants.Method.INSERT,
        constants.Channel.ONLINE,
        constants.FeedType.PRIMARY,
    )

  def test_run_process_should_call_content_api_with_delete_when_operation_is_delete(
      self):
    self.test_client.post(
        DELETE_URL, data=REQUEST_BODY_ONLINE, headers=REQUEST_HEADERS
    )

    self.mock_content_api_client.return_value.process_items.assert_any_call(
        mock.ANY,
        mock.ANY,
        mock.ANY,
        constants.Method.DELETE,
        constants.Channel.ONLINE,
        constants.FeedType.PRIMARY,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'bad_request',
          'reason': 'BAD REQUEST',
          'status': http.HTTPStatus.BAD_REQUEST,
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'internal_server_error',
          'reason': 'INTERNAL SERVER ERROR',
          'status': http.HTTPStatus.INTERNAL_SERVER_ERROR,
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'bad_request_local',
          'reason': 'BAD REQUEST',
          'status': http.HTTPStatus.BAD_REQUEST,
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
      {
          'testcase_name': 'internal_server_error_local',
          'reason': 'INTERNAL SERVER ERROR',
          'status': http.HTTPStatus.INTERNAL_SERVER_ERROR,
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_the_same_error_when_content_api_call_returns_error_and_retry_is_suggested(
      self, reason, status, request_headers, request_body
  ):
    with mock.patch('content_api_client.suggest_retry') as suggest_retry:
      suggest_retry.return_value = True
      self.mock_content_api_client.return_value.process_items.side_effect = (
          errors.HttpError(mock.MagicMock(status=status, reason=reason), b''))

      response = self.test_client.post(
          INSERT_URL, data=request_body, headers=request_headers
      )

      self.assertEqual(status, response.status_code)
      self.assertEqual(reason, response.data.decode())

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_error_when_content_api_call_returns_error_and_retry_is_not_suggested(
      self, request_headers, request_body
  ):
    with mock.patch('content_api_client.suggest_retry') as suggest_retry:
      suggest_retry.return_value = False
      self.mock_content_api_client.return_value.process_items.side_effect = (
          errors.HttpError(
              mock.MagicMock(
                  status=http.HTTPStatus.PAYMENT_REQUIRED,
                  reason='Payment Required'), b''))

      response = self.test_client.post(
          INSERT_URL, data=request_body, headers=request_headers
      )

      self.assertEqual(http.HTTPStatus.PAYMENT_REQUIRED, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_timeout_error_when_content_api_call_returns_socket_timeout_error(
      self, request_headers, request_body
  ):
    self.mock_content_api_client.return_value.process_items.side_effect = (
        socket.timeout())

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.assertEqual(http.HTTPStatus.REQUEST_TIMEOUT, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'channel': constants.Channel.ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'channel': constants.Channel.LOCAL,
      },
  )
  def test_run_process_should_log_error_when_max_retry_attempts_exhausted(
      self, request_headers, request_body, channel
  ):
    max_retry_count = 5
    http_error = errors.HttpError(
        mock.MagicMock(
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            reason='Server got itself in trouble'), b'')
    self.mock_content_api_client.return_value.process_items.side_effect = (
        http_error)

    with self.assertLogs(level='ERROR') as log:
      request_headers[HEADER_NAME_FOR_TASK_EXECUTION_COUNT] = (
          f'{max_retry_count}'
      )
      self.test_client.post(
          INSERT_URL, data=request_body, headers=request_headers
      )

      self.assertIn(
          (
              'ERROR:root:Batch #1 with operation upsert, initiation timestamp '
              f'{DUMMY_TIMESTAMP}, and channel {channel.value} '
              'failed and will not be retried. '
              f'Error: {http_error}'
          ),
          log.output,
      )

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_return_ok_when_execution_count_header_missing_and_content_api_call_returns_success(
      self, request_headers, request_body
  ):
    del request_headers[HEADER_NAME_FOR_TASK_EXECUTION_COUNT]
    from_service_account_json = (
        self.mock_bq_client.from_service_account_json.return_value
    )
    from_service_account_json.load_items.return_value = DUMMY_ROWS

    response = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'channel': constants.Channel.ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'channel': constants.Channel.LOCAL,
      },
  )
  def test_run_process_should_log_error_when_execution_count_header_missing_and_content_api_call_returns_error(
      self, request_headers, request_body, channel
  ):
    del request_headers[HEADER_NAME_FOR_TASK_EXECUTION_COUNT]
    http_error = errors.HttpError(
        mock.MagicMock(
            status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            reason='Server got itself in trouble'), b'')
    self.mock_content_api_client.return_value.process_items.side_effect = (
        http_error)

    with self.assertLogs(level='ERROR') as log:
      self.test_client.post(
          INSERT_URL, data=request_body, headers=request_headers
      )

      self.assertIn(
          (
              'ERROR:root:Batch #1 with operation upsert, initiation timestamp '
              f'{DUMMY_TIMESTAMP}, and channel {channel.value} '
              'failed and will not be retried. '
              f'Error: {http_error}'
          ),
          log.output,
      )

  @parameterized.named_parameters(
      {
          'testcase_name': 'feed_primary_and_channel_online',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'channel': constants.Channel.ONLINE,
      },
      {
          'testcase_name': 'feed_primary_and_channel_local',
          'request_headers': REQUEST_HEADERS,
          'request_body': json.dumps({
              'start_index': DUMMY_START_INDEX,
              'batch_size': DUMMY_BATCH_SIZE,
              'timestamp': DUMMY_TIMESTAMP,
              'channel': constants.Channel.LOCAL.value,
          }),
          'channel': constants.Channel.LOCAL,
      },
      {
          'testcase_name': 'feed_local_and_channel_local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'channel': constants.Channel.LOCAL,
      },
  )
  def test_run_process_should_record_result_when_content_api_call_returns_ok(
      self, request_headers, request_body, channel
  ):
    expected_batch_id = int(DUMMY_START_INDEX / DUMMY_BATCH_SIZE) + 1
    expected_result = process_result.ProcessResult(DUMMY_SUCCESSES,
                                                   DUMMY_FAILURES, [])

    self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_called_once_with(
        channel,
        DUMMY_OPERATION,
        expected_result,
        DUMMY_TIMESTAMP,
        expected_batch_id,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'channel': constants.Channel.ONLINE,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'channel': constants.Channel.LOCAL,
      },
  )
  def test_run_process_should_record_that_all_items_failed_when_content_api_call_returns_error(
      self, request_headers, request_body, channel
  ):
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
    from_service_account_json = (
        self.mock_bq_client.from_service_account_json.return_value
    )
    from_service_account_json.load_items.return_value = DUMMY_ROWS

    self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_recorder.from_service_account_json.return_value.insert_result.assert_called_once_with(
        channel,
        DUMMY_OPERATION,
        expected_result,
        DUMMY_TIMESTAMP,
        expected_batch_id,
    )

  @parameterized.named_parameters(
      {
          'testcase_name': 'insert_main',
          'url': INSERT_URL,
      },
      {
          'testcase_name': 'prevent_expiring_main',
          'url': PREVENT_EXPIRING_URL,
      },
  )
  @mock.patch.dict(
      os.environ,
      {
          'SHOPTIMIZER_API_INTEGRATION_ON': 'True',
      },
  )
  def test_run_process_should_call_shoptimizer(self, url):
    self.test_client.post(
        url, data=REQUEST_BODY_ONLINE, headers=REQUEST_HEADERS
    )

    self.mock_shoptimizer_client.return_value.shoptimize.assert_called_once()

  @parameterized.named_parameters(
      {
          'testcase_name': 'delete_main',
          'url': DELETE_URL,
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
      },
      {
          'testcase_name': 'insert_local',
          'url': INSERT_URL,
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
      },
  )
  def test_run_process_should_not_call_shoptimizer(
      self, url, request_headers, request_body
  ):
    self.test_client.post(url, data=request_body, headers=request_headers)

    self.mock_shoptimizer_client.return_value.shoptimize.assert_not_called()

  @parameterized.named_parameters(
      {
          'testcase_name': 'main',
          'request_headers': REQUEST_HEADERS,
          'request_body': REQUEST_BODY_ONLINE,
          'dataset_id': constants.DATASET_ID_FOR_PROCESSING,
      },
      {
          'testcase_name': 'local',
          'request_headers': REQUEST_HEADERS_LOCAL,
          'request_body': REQUEST_BODY_LOCAL,
          'dataset_id': (
              constants.DATASET_ID_FOR_PROCESSING
              + main.LOCAL_SUFFIX_FOR_BIGQUERY
          ),
      },
  )
  def test_load_items_from_correct_bigquery_table(
      self, request_headers, request_body, dataset_id
  ):
    _ = self.test_client.post(
        INSERT_URL, data=request_body, headers=request_headers
    )

    self.mock_bq_client.from_service_account_json.assert_called_with(
        service_account_path=mock.ANY,
        dataset_id=dataset_id,
        table_id=mock.ANY,
    )
