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

"""Tests for App Engine server of initiator service."""

import collections
import http
import os
from typing import Any, Mapping, Union
import unittest
import unittest.mock as mock

from google.cloud import exceptions

import bigquery_client
import main

_START_PATH = '/start'
_DUMMY_DELETE_COUNT = 1
_DUMMY_EXPIRING_COUNT = 2
_DUMMY_UPSERT_COUNT = 3

_TEST_PROJECT_ID = 'test-project-id'
_TEST_REGION = 'test-region'
_TEST_TRIGGER_COMPLETION_BUCKET = 'test-trigger-completion-bucket'
_TEST_LOCK_BUCKET = 'test-lock-bucket'

_TEST_SERVICE_ACCOUNT = main._SERVICE_ACCOUNT
_TEST_TARGET_URL_INSERT = main._TARGET_URL_INSERT
_TEST_TABLE_ID_ITEMS = main._TABLE_ID_ITEMS
_TEST_DATASET_ID = main._DATASET_ID_FEED_DATA
_TEST_DATASET_ID_LOCAL = main._DATASET_ID_FEED_DATA_LOCAL
_TEST_QUEUE_NAME = main._QUEUE_NAME
_TEST_QUEUE_NAME_LOCAL = main._QUEUE_NAME_LOCAL

_TEST_CHANNEL_LOCAL = 'local'
_TEST_CHANNEL_ONLINE = 'online'


def _build_headers_for_start(local_inventory_feed_enabled: bool = False):
  """Creates headers for /start.

  Args:
    local_inventory_feed_enabled: True if the incoming request is for local
      inventory feed. Otherwise, False.

  Returns:
    A dictionary for headers.
  """
  if local_inventory_feed_enabled:
    queue_name = _TEST_QUEUE_NAME_LOCAL
  else:
    queue_name = _TEST_QUEUE_NAME
  return {'X-Appengine-Queuename': queue_name}


def _build_request_body_for_start(
    delete_count: Union[int, str], expiring_count: Union[int, str],
    upsert_count: Union[int, str]) -> Mapping[str, Any]:
  """Create a request body for /start.

  Args:
    delete_count: fake number of items to be deleted.
    expiring_count: fake number of items that are due to expire.
    upsert_count: fake number of items to be upserted.

  Returns:
    collections.defaultdict, request body for /start.
  """
  request_body = collections.defaultdict(lambda: collections.defaultdict(dict))
  request_body['deleteCount'] = delete_count
  request_body['expiringCount'] = expiring_count
  request_body['upsertCount'] = upsert_count
  return request_body


@mock.patch.dict(
    os.environ, {
        'PROJECT_ID': _TEST_PROJECT_ID,
        'REGION': _TEST_REGION,
        'USE_LOCAL_INVENTORY_ADS': 'True',
        'TRIGGER_COMPLETION_BUCKET': _TEST_TRIGGER_COMPLETION_BUCKET,
        'LOCK_BUCKET': _TEST_LOCK_BUCKET,
    })
class MainTest(unittest.TestCase):

  def setUp(self):
    super(MainTest, self).setUp()
    main.app.testing = True
    self.test_app_client = main.app.test_client()
    self.bigquery_client = mock.patch(
        'bigquery_client.BigQueryClient.from_service_account_json').start()
    self.tasks_client = mock.patch(
        'tasks_client.TasksClient.from_service_account_json').start()
    self.storage_client = mock.patch(
        'storage_client.StorageClient.from_service_account_json').start()
    self.pubsub_client = mock.patch(
        'pubsub_client.PubSubClient.from_service_account_json').start()
    mock_cloud_logging = mock.patch('main.cloud_logging')
    mock_cloud_logging.start()
    self.addCleanup(mock.patch.stopall)

  def test_ok_returned_when_request_body_valid(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)

    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )

    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  def test_items_table_deleted_and_bad_request_returned_when_body_not_json(
      self):
    headers = _build_headers_for_start()
    response = self.test_app_client.post(
        _START_PATH, data='not valid json', headers=headers
    )
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_eof_deleted_and_bad_request_returned_when_body_not_json(self):
    headers = _build_headers_for_start()
    response = self.test_app_client.post(
        _START_PATH, data='not valid json', headers=headers
    )
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_items_table_deleted_and_bad_request_returned_when_upsert_count_invalid(
      self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count='not a number',
    )
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_eof_deleted_and_bad_request_returned_when_upsert_count_invalid(
      self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count='not a number')
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_upsert_table_created_when_upsert_count_is_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    upsert_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_UPSERT, _TEST_PROJECT_ID, _TEST_DATASET_ID
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        upsert_query)

  def test_upsert_table_not_created_when_upsert_count_is_not_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    upsert_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_UPSERT, _TEST_PROJECT_ID, _TEST_DATASET_ID
    )
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        upsert_query)

  def test_delete_table_created_when_delete_count_is_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    delete_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_DELETE, _TEST_PROJECT_ID, _TEST_DATASET_ID
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        delete_query)

  def test_delete_table_not_created_when_delete_count_is_not_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    delete_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_DELETE, _TEST_PROJECT_ID, _TEST_DATASET_ID
    )
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        delete_query)

  def test_prevent_expiring_table_created_when_expiring_count_is_positive(
      self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    prevent_expiring_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_PREVENT_EXPIRING,
        _TEST_PROJECT_ID,
        _TEST_DATASET_ID,
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        prevent_expiring_query)

  def test_prevent_expiring_table_not_created_when_expiring_count_is_not_positive(
      self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    prevent_expiring_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_PREVENT_EXPIRING,
        _TEST_PROJECT_ID,
        _TEST_DATASET_ID,
    )
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        prevent_expiring_query)

  def test_items_table_deleted_when_any_tasks_started_is_false(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.bigquery_client.return_value.delete_table.assert_called_once()

  def test_eof_deleted_when_any_tasks_started_is_false(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.delete_eof_lock.assert_called_once()

  def test_no_tasks_created_when_no_content_api_calls_required(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_not_called()

  def test_tasks_created_when_upsert_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_UPSERT_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=mock.ANY)

  def test_tasks_created_when_delete_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_DELETE_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=mock.ANY)

  def test_tasks_created_when_expiring_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_EXPIRING_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=_TEST_CHANNEL_ONLINE)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_EXPIRING_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=_TEST_CHANNEL_LOCAL)
    self.assertEqual(self.tasks_client.return_value.push_tasks.call_count, 2)

  def test_a_task_created_with_local_inventory_feed_and_lia(self):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_UPSERT_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=_TEST_CHANNEL_LOCAL,
    )
    self.assertEqual(self.tasks_client.return_value.push_tasks.call_count, 1)

  @mock.patch.dict(
      os.environ,
      {
          'USE_LOCAL_INVENTORY_ADS': 'False',
      },
  )
  def test_a_task_created_with_primary_feed_and_no_lia(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_UPSERT_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY,
        channel=_TEST_CHANNEL_ONLINE)
    self.assertEqual(self.tasks_client.return_value.push_tasks.call_count, 1)

  @mock.patch.dict(
      os.environ,
      {
          'USE_LOCAL_INVENTORY_ADS': 'False',
      },
  )
  def test_raises_error_when_create_task_with_local_inventory_feed_and_no_lia(
      self,
  ):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT
    )

    with self.assertRaises(main.LocalInventoryFeedEnabledButLIADisabledError):
      self.test_app_client.post(_START_PATH, json=request_body, headers=headers)

  def test_eof_not_uploaded_when_no_content_api_call_required(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.upload_eof.assert_not_called()

  def test_mailer_triggered_when_no_content_api_call_required(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.pubsub_client.return_value.trigger_result_email.assert_called_once()

  def test_eof_uploaded_when_upsert_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_uploaded_when_delete_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_uploaded_when_expiring_count_positive(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_only_uploaded_once(self):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_items_table_deleted_when_creating_processing_table_fails(self):
    self.bigquery_client.return_value.initialize_dataset_and_table.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_eof_lock_deleted_when_creating_processing_table_fails(self):
    self.bigquery_client.return_value.initialize_dataset_and_table.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_items_table_deleted_when_task_creation_fails(self):
    self.tasks_client.return_value.push_tasks.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_eof_deleted_when_task_creation_fails(self):
    self.tasks_client.return_value.push_tasks.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(
        _START_PATH, json=request_body, headers=headers
    )
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_upsert_table_created_when_local_inventory_feed_is_given(self):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT
    )
    upsert_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_UPSERT,
        _TEST_PROJECT_ID,
        _TEST_DATASET_ID_LOCAL,
    )

    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)

    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        upsert_query
    )

  def test_delete_table_created_when_local_inventory_feed_is_given(self):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0
    )
    delete_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_DELETE,
        _TEST_PROJECT_ID,
        _TEST_DATASET_ID_LOCAL,
    )

    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)

    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        delete_query
    )

  def test_prevent_expiring_table_not_created_when_local_inventory_feed_is_given(
      self,
  ):
    headers = _build_headers_for_start()
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    prevent_expiring_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_PREVENT_EXPIRING,
        _TEST_PROJECT_ID,
        _TEST_DATASET_ID_LOCAL,
    )
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        prevent_expiring_query,
    )

  @mock.patch('tasks_client.TasksClient')
  def test_create_tasks_called_with_correct_queue_name_when_local_inventory_feed_is_given(
      self, mock_tasks_client
  ):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT
    )
    self.test_app_client.post(_START_PATH, json=request_body, headers=headers)
    mock_tasks_client.from_service_account_json.assert_called_with(
        _TEST_SERVICE_ACCOUNT,
        url=_TEST_TARGET_URL_INSERT,
        project_id=_TEST_PROJECT_ID,
        location=_TEST_REGION,
        queue_name=_TEST_QUEUE_NAME_LOCAL,
    )

  @mock.patch('bigquery_client.BigQueryClient')
  def test_delete_items_table_called_when_local_inventory_feed_is_given(
      self, bq_client
  ):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    _ = self.test_app_client.post(
        _START_PATH, data='not valid json', headers=headers
    )
    bq_client.from_service_account_json.assert_called_with(
        _TEST_SERVICE_ACCOUNT, _TEST_DATASET_ID_LOCAL, _TEST_TABLE_ID_ITEMS
    )

  @mock.patch('storage_client.StorageClient')
  def test_delete_eof_lock_called_when_local_inventory_feed_is_given(
      self, storage_client
  ):
    headers = _build_headers_for_start(local_inventory_feed_enabled=True)
    _ = self.test_app_client.post(
        _START_PATH, data='not valid json', headers=headers
    )
    storage_client.from_service_account_json.assert_called_with(
        _TEST_SERVICE_ACCOUNT, _TEST_LOCK_BUCKET + main.LOCAL_SUFFIX
    )

  def assert_not_called_with(self, mock_obj, *unexpected_args,
                             **unexpected_kwargs):
    for call in mock_obj.call_args_list:
      args, kwargs = call
      for arg in args:
        if arg in unexpected_args:
          raise AssertionError(f'Mock was called with {arg}')
      for key, value in kwargs.items():
        if key in unexpected_kwargs.keys() and unexpected_kwargs[key] == value:
          raise AssertionError(f'Mock was called with {key}={value}')
