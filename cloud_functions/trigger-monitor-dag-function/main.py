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

"""Cloud Function that triggers Cloud Composer (Airflow) on GCP."""
import enum
import logging
import os
from typing import Any, Dict

from google.api_core import exceptions
import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

_HTTP_REQUEST_METHODS = enum.Enum(
    'HTTP_REQUEST_METHODS', 'GET OPTIONS HEAD POST PUT PATCH DELETE'
)
AUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


def trigger_dag(
    event: Dict[str, Any], context: 'google.cloud.functions.Context'
) -> None:
  """Entry point for the main feed (non-local) version of the Cloud Function."""
  del context
  post_to_composer(event)


def trigger_dag_local(
    event: Dict[str, Any], context: 'google.cloud.functions.Context'
) -> None:
  """Entry point for the local version of the Cloud Function."""
  del context
  post_to_composer(event)


def post_to_composer(event: Dict[str, Any]) -> None:
  """Makes a POST request to the Composer DAG Trigger API.

  When called via Google Cloud Functions (GCF),
  event and context are Background function parameters.

  Args:
    event:  The dictionary with data specific to this type of event. The `data`
      field contains a description of the event in the Cloud Storage
      `object` format described here:
      https://cloud.google.com/storage/docs/json_api/v1/objects#resource

  Returns:
    None. The output is written to Cloud logging.
  """
  webserver_id = os.environ.get('WEBSERVER_ID')
  dag_name = os.environ.get('DAG_NAME')

  print(
      'Attempting to trigger the Cloud Composer (Airflow) DAG named '
      f'"{dag_name}" at {webserver_id}...'
  )

  json_data = {'conf': event}
  webserver_url = os.path.join(webserver_id, 'api/v1/dags', dag_name, 'dagRuns')
  _make_composer_web_server_request(
      webserver_url, method='POST', json=json_data
  )


def _make_composer_web_server_request(
    url: str, method: str = _HTTP_REQUEST_METHODS.GET.name, **kwargs: Any
) -> str:
  """Make a request to Cloud Composer 2 environment's web server.

  Args:
    url: The URL for Composer 2 endpoint.
    method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
      'PATCH', 'DELETE')
    **kwargs: Any of the parameters defined for the request function:
              https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.

  Raises:
    HTTPError: The HTTP request has a bad response.

  Returns:
    The response from Composer2.
  """

  try:
    _HTTP_REQUEST_METHODS[method]
  except KeyError:
    logging.error(
        exceptions.BadRequest(
            f'An invalid HTTP request method {method} was supplied.'
        )
    )
    return

  # Sets the default timeout, if missing.
  kwargs.setdefault('timeout', 90)

  authed_session = AuthorizedSession(CREDENTIALS)
  response = authed_session.request(method, url, **kwargs)

  if response.status_code == 403:
    err_msg = (
        (
            'You do not have permission to perform this operation. '
            'Check Airflow RBAC roles for your account. '
            '%s / %s'
        ),
        response.headers,
        response.text,
    )
    logging.error(err_msg)
    raise requests.HTTPError(err_msg)
  elif response.status_code != 200:
    logging.error(response.text)
    response.raise_for_status()
  else:
    return response.text
