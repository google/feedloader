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

"""Tests for App Engine server of initiator service."""

import collections
import http
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
    self.addCleanup(mock.patch.stopall)

  def test_ok_returned_when_request_body_valid(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.assertEqual(http.HTTPStatus.OK, response.status_code)

  def test_items_table_deleted_and_bad_request_returned_when_body_not_json(
      self):
    response = self.test_app_client.post(_START_PATH, data='not valid json')
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_eof_deleted_and_bad_request_returned_when_body_not_json(self):
    response = self.test_app_client.post(_START_PATH, data='not valid json')
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_items_table_deleted_and_bad_request_returned_when_upsert_count_invalid(
      self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count='not a number',
    )
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_eof_deleted_and_bad_request_returned_when_upsert_count_invalid(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count='not a number')
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.BAD_REQUEST, response.status_code)

  def test_upsert_table_created_when_upsert_count_is_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    upsert_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_UPSERT, main._PROJECT_ID)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        upsert_query)

  def test_upsert_table_not_created_when_upsert_count_is_not_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    upsert_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_UPSERT, main._PROJECT_ID)
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        upsert_query)

  def test_delete_table_created_when_delete_count_is_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    delete_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_DELETE, main._PROJECT_ID)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        delete_query)

  def test_delete_table_not_created_when_delete_count_is_not_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body)
    delete_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_DELETE, main._PROJECT_ID)
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        delete_query)

  def test_prevent_expiring_table_created_when_expiring_count_is_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    prevent_expiring_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_PREVENT_EXPIRING, main._PROJECT_ID)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.initialize_dataset_and_table.assert_any_call(
        prevent_expiring_query)

  def test_prevent_expiring_table_not_created_when_expiring_count_is_not_positive(
      self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    prevent_expiring_query = bigquery_client.generate_query_string(
        main._QUERY_FILEPATH_FOR_PREVENT_EXPIRING, main._PROJECT_ID)
    self.assert_not_called_with(
        self.bigquery_client.return_value.initialize_dataset_and_table,
        prevent_expiring_query)

  def test_items_table_deleted_when_any_tasks_started_is_false(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.delete_table.assert_called_once()

  def test_eof_deleted_when_any_tasks_started_is_false(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.delete_eof_lock.assert_called_once()

  def test_no_tasks_created_when_no_content_api_calls_required(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.tasks_client.return_value.push_tasks.assert_not_called()

  def test_tasks_created_when_upsert_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_UPSERT_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY)

  def test_tasks_created_when_delete_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_DELETE_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY)

  def test_tasks_created_when_expiring_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.tasks_client.return_value.push_tasks.assert_any_call(
        total_items=_DUMMY_EXPIRING_COUNT,
        batch_size=mock.ANY,
        timestamp=mock.ANY)

  def test_eof_not_uploaded_when_no_content_api_call_required(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.upload_eof.assert_not_called()

  def test_mailer_triggered_when_no_content_api_call_required(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.pubsub_client.return_value.trigger_result_email.assert_called_once()

  def test_eof_uploaded_when_upsert_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=0, upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_uploaded_when_delete_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT, expiring_count=0, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_uploaded_when_expiring_count_positive(self):
    request_body = _build_request_body_for_start(
        delete_count=0, expiring_count=_DUMMY_EXPIRING_COUNT, upsert_count=0)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_eof_only_uploaded_once(self):
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.upload_eof.assert_called_once()

  def test_items_table_deleted_when_creating_processing_table_fails(self):
    self.bigquery_client.return_value.initialize_dataset_and_table.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_eof_lock_deleted_when_creating_processing_table_fails(self):
    self.bigquery_client.return_value.initialize_dataset_and_table.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.delete_eof_lock.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_items_table_deleted_when_task_creation_fails(self):
    self.tasks_client.return_value.push_tasks.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.bigquery_client.return_value.delete_table.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

  def test_eof_deleted_when_task_creation_fails(self):
    self.tasks_client.return_value.push_tasks.side_effect = exceptions.GoogleCloudError(
        'Dummy message')
    request_body = _build_request_body_for_start(
        delete_count=_DUMMY_DELETE_COUNT,
        expiring_count=_DUMMY_EXPIRING_COUNT,
        upsert_count=_DUMMY_UPSERT_COUNT)
    response = self.test_app_client.post(_START_PATH, json=request_body)
    self.storage_client.return_value.delete_eof.assert_called_once()
    self.assertEqual(http.HTTPStatus.INTERNAL_SERVER_ERROR,
                     response.status_code)

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
