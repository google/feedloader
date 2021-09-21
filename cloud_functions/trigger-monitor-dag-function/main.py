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

"""Cloud Function that triggers Cloud Composer (Airflow) on GCP."""
import enum
import logging
import os
from typing import Any, Dict

from google.api_core import exceptions
from google.auth.transport.requests import Request
from google.oauth2 import id_token

import requests

_HTTP_REQUEST_METHODS = enum.Enum('HTTP_REQUEST_METHODS',
                                  'GET OPTIONS HEAD POST PUT PATCH DELETE')


def trigger_dag(event: Dict[str, Any],
                context: 'google.cloud.functions.Context') -> None:
  """Makes a POST request to the Composer DAG Trigger API.

  When called via Google Cloud Functions (GCF),
  event and context are Background function parameters.

  Args:
    event:  The dictionary with data specific to this type of event. The `data`
      field contains a description of the event in the Cloud Storage
      `object` format described here:
      https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    context: Metadata of triggering event.

  Returns:
    None. The output is written to Cloud logging.
  """
  del context
  client_id = os.environ.get('CLIENT_ID')
  webserver_id = os.environ.get('WEBSERVER_ID')
  dag_name = os.environ.get('DAG_NAME')
  json_data = {'conf': event, 'replace_microseconds': 'false'}
  webserver_url = f'{webserver_id}/api/experimental/dags/{dag_name}/dag_runs'
  make_iap_request(webserver_url, client_id, method='POST', json=json_data)


def make_iap_request(url: str,
                     client_id: str,
                     method=_HTTP_REQUEST_METHODS.GET.name,
                     **kwargs: str) -> str:
  """Makes a request to an application protected by Identity-Aware Proxy.

  Args:
    url: The Identity-Aware Proxy-protected URL to fetch.
    client_id: The client ID used by Identity-Aware Proxy.
    method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
      'PATCH', 'DELETE')
    **kwargs: Any of the parameters defined for the request function:
              https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.

  Raises:
    GatewayTimeout: The call to the Google API was unauthorized.
    GoogleAPICallError: The call to the Google API endpoint failed.

  Returns:
    The page body, or raises an exception if the page couldn't be retrieved.
  """
  if method not in _HTTP_REQUEST_METHODS._value2member_map_:  
    logging.error(
        exceptions.BadRequest(
            f'An invalid HTTP request method {method} was supplied.'))
    return

  # Sets the default timeout, if missing.
  if 'timeout' not in kwargs:
    kwargs.set_default('timeout', 90)

  google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)
  resp = requests.request(
      method,
      url,
      headers={'Authorization': f'Bearer {google_open_id_connect_token}'},
      **kwargs)

  if resp.status_code == 403:
    logging.error(
        exceptions.Forbidden(
            'Service account does not have '
            'permission to access the IAP-protected application.'))
  elif resp.status_code != 200:
    logging.error(
        exceptions.GoogleAPICallError(
            f'Bad response from application: Status: {resp.status_code}, '
            f'Headers: {resp.headers}, Text: {resp.text}'))
  else:
    return resp.text
