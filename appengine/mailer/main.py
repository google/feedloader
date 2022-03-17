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

"""Sends a notification of completion of the Shopping Feed uploader to the specified email destination."""

import datetime
import httplib
import json
import logging
import os

import flask
from google.appengine.api import app_identity
from google.appengine.api import mail
import jinja2
import pytz

from models import run_result

app = flask.Flask(__name__)

_JAPAN_TIMEZONE = 'Asia/Tokyo'

_CONTENT_API_OPERATION_UPSERT = 'upsert'
_CONTENT_API_OPERATION_DELETE = 'delete'
_CONTENT_API_OPERATION_PREVENT_EXPIRING = 'prevent_expiring'

_OPERATION_DESCRIPTIONS = {
    _CONTENT_API_OPERATION_UPSERT: 'Products inserted/updated',
    _CONTENT_API_OPERATION_DELETE: 'Products deleted',
    _CONTENT_API_OPERATION_PREVENT_EXPIRING: 'Expiration dates extended'
}

_OPERATIONS = (_CONTENT_API_OPERATION_UPSERT, _CONTENT_API_OPERATION_DELETE,
               _CONTENT_API_OPERATION_PREVENT_EXPIRING)

# Get environment variables
_PUBSUB_VERIFICATION_TOKEN = 'PUBSUB_VERIFICATION_TOKEN'
_EMAIL_TO = 'EMAIL_TO'
_USE_LOCAL_INVENTORY_ADS = 'USE_LOCAL_INVENTORY_ADS'


@app.route('/health', methods=['GET'])
def start():
  return 'OK', httplib.OK


@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
  """Validates the request came from pubsub and sends the completion email."""
  if flask.request.args.get('token') != _load_environment_variable(
      _PUBSUB_VERIFICATION_TOKEN):
    return 'Unauthorized', httplib.UNAUTHORIZED
  request_body = json.loads(flask.request.data.decode('utf-8'))
  try:
    run_results_dict = _extract_run_result(request_body)
  except ValueError:
    logging.error('Request body is not JSON-encodable: %s', request_body)
    return 'Invalid request body', httplib.BAD_REQUEST
  run_results = _get_run_result_list(run_results_dict)

  total_items_processed = {}
  for channel in _get_channels():
    total_items_processed[channel] = sum(
        result.get_total_count() for result in run_results.get(channel, []))

  current_datetime = datetime.datetime.now(pytz.timezone(_JAPAN_TIMEZONE))

  jinja_environment = jinja2.Environment(
      loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
      extensions=['jinja2.ext.autoescape'],
      autoescape=True)

  template_values = {
      'currentMonth':
          current_datetime.strftime('%B'),
      'currentYear':
          current_datetime.strftime('%Y'),
      'fullTimestamp':
          '%s (%s)' %
          (current_datetime.strftime('%B %d, %Y %H:%M:%S'), _JAPAN_TIMEZONE),
      'projectId':
          _project_id(),
      'runResults':
          run_results,
      'totalItemsProcessed':
          total_items_processed,
      'useLocalInventoryAds':
          _use_local_inventory_ads(),
  }

  template = jinja_environment.get_template('completion_mail.html')
  html_body = template.render(template_values)
  message = mail.EmailMessage(
      sender='no-reply@{0}.appspotmail.com'.format(_project_id()),
      subject='Shopping Feed Processing Completed',
      to=_load_environment_variable(_EMAIL_TO),
      html=html_body)
  message.send()
  return 'OK!', httplib.OK


def _extract_run_result(request_body):
  """Extracts run results from the request body came from Cloud Pub/Sub.

  Args:
    request_body: dict, a request body came from Cloud Pub/Sub.

  Returns:
    a dict containing the result of a run.
  """
  run_results_str = request_body.get('message',
                                     {}).get('attributes',
                                             {}).get('content_api_results',
                                                     '[]')
  run_results_dict = json.loads(run_results_str.decode('utf-8'))
  return run_results_dict


def _get_run_result_list(run_results_dict):
  """Converts run results from a dictionary to a list of run_result objects.

  Args:
    run_results_dict: A dictionary of run results.

  Returns:
    A dictionary containing RunResult objects. Key is a channel and value is a
    RunResult object.
  """
  run_results = {}
  for channel in _get_channels():
    rows_for_channel = [
        row for row in run_results_dict if row.get('channel') == channel
    ]
    run_results_for_channel = []
    for operation in _OPERATIONS:
      # Set a dict without run result numbers by default. The result table in
      # the email shows zero for the operation.
      row_for_operation = {'channel': channel, 'operation': operation}
      for row in rows_for_channel:
        if row.get('operation') == operation:
          row_for_operation = row
      row_for_operation['description'] = _OPERATION_DESCRIPTIONS.get(
          operation, '')
      run_results_for_operation = run_result.RunResult.from_dict(
          row_for_operation)
      run_results_for_channel.append(run_results_for_operation)
    run_results[channel] = run_results_for_channel

  return run_results


def _get_channels():
  """Returns a list of Shopping channels based on the environment value."""
  return ['online', 'local'] if _use_local_inventory_ads() else ['online']


def _project_id():
  """Returns project id."""
  return app_identity.get_application_id()


def _use_local_inventory_ads():
  """Returns boolean value of whether to use local channel or not."""
  use_local_inventory_ads = _load_environment_variable(_USE_LOCAL_INVENTORY_ADS)
  return True if use_local_inventory_ads.lower() == 'true' else False


def _load_environment_variable(key):
  """Returns a value of environment variable."""
  return os.getenv(key)


if __name__ == '__main__':
  app.run(host='127.0.0.1', port=8080, debug=True)
