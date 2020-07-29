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

"""Sends Cloud Build completion status to the specified email addresses."""

import base64
import httplib
import json
import logging
import os

import flask
from google.appengine.api import mail
import jinja2

app = flask.Flask(__name__)

# The build statuses that will trigger a notification email.
_STATUSES_TO_REPORT = ('SUCCESS', 'FAILURE')
_TAGS_TO_REPORT = frozenset(['feedloader'])

# Get environment variables
_EMAIL_TO = os.getenv('EMAIL_TO')


@app.route('/health', methods=['GET'])
def health():
  """Checks the deployed application is running correctly."""
  return 'OK', httplib.OK


@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
  """Sends a notification email based on the Cloud Build PubSub message.

  The email will not be sent if the build status is not in the list of
  _STATUSES_TO_REPORT. Also, at least one of the build tags must be contained
  within _TAGS_TO_REPORT, or the email will not be sent. Emails can also be
  disabled by setting the SHOULD_SEND_EMAIL environment variable to False.

  Returns:
    A str indicating the result of the call and an http status code.
  """
  if not _EMAIL_TO:
    logging.info('Build email not sent: EMAIL_TO is empty')
    return 'OK!', httplib.OK

  request_body = json.loads(flask.request.data.decode('utf-8'))
  message = request_body.get('message', {})
  attributes = message.get('attributes', {})
  decoded_data = base64.decodebytes(message.get('data', ''))
  data_dict = json.loads(decoded_data)

  status = attributes.get('status', '').upper()
  tags = set(data_dict.get('tags', []))

  if status not in _STATUSES_TO_REPORT:
    logging.info('Build email not sent: status not in statuses to report: %s',
                 status)
    return 'OK!', httplib.OK

  if not tags.intersection(_TAGS_TO_REPORT):
    logging.info('Build email not sent: build tag not in TAGS_TO_REPORT')
    return 'OK!', httplib.OK

  build_id = attributes.get('buildId', '')
  build_logs = data_dict.get('logUrl', '')
  project_id = data_dict.get('projectId', '')
  publish_time = message.get('publish_time', '')

  jinja_environment = jinja2.Environment(
      loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
      extensions=['jinja2.ext.autoescape'],
      autoescape=True)

  template_values = {
      'build_id': build_id,
      'build_logs': build_logs,
      'project_id': project_id,
      'publish_time': publish_time,
      'status': status,
  }

  template = jinja_environment.get_template('build_report_template.html')
  html_body = template.render(template_values)
  message = mail.EmailMessage(
      sender='no-reply@{0}.appspotmail.com'.format(project_id),
      subject='Feedloader Build Result: {}'.format(status),
      to=_EMAIL_TO,
      html=html_body)
  message.send()

  return 'OK!', httplib.OK


if __name__ == '__main__':
  app.run(host='127.0.0.1', port=8080, debug=True)
