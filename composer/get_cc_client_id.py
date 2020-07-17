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

# python3
"""Get the client ID associated with a Cloud Composer environment."""


import argparse
import requests
from urllib import parse
import google.auth
import google.auth.transport.requests

_CLOUD_API_AUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
_CLOUD_COMPOSER_ENVIRONMENT_BASE_URL = 'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}/environments/{}'


def get_client_id(project_id, location, composer_environment):
  """Retrieves the Client ID of the Cloud Composer environment.

  Args:
    project_id: String, The ID of the GCP Project.
    location: String, Region of the Cloud Composer environent.
    composer_environment: String, Name of the Cloud Composer environent.
  """

  credentials, _ = google.auth.default(scopes=[_CLOUD_API_AUTH_SCOPE])
  authed_session = google.auth.transport.requests.AuthorizedSession(credentials)
  environment_url = _CLOUD_COMPOSER_ENVIRONMENT_BASE_URL.format(
      project_id, location, composer_environment)
  composer_response = authed_session.request('GET', environment_url)
  environment_data = composer_response.json()
  airflow_uri = environment_data['config']['airflowUri']
  redirect_response = requests.get(airflow_uri, allow_redirects=False)
  redirect_location = redirect_response.headers['location']
  parsed = parse.urlparse(redirect_location)
  query_string = parse.parse_qs(parsed.query)
  client_id = query_string['client_id'][0]
  # Because this will be used via a bash script to save a local variable,
  # print instead of return.
  print(client_id)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
      'project_id', help='Your Project ID.', type=str)
  parser.add_argument(
      'location',
      help='Region of the Cloud Composer environent.',
      type=str)
  parser.add_argument(
      'composer_environment',
      help='Name of the Cloud Composer environent.',
      type=str)

  args = parser.parse_args()
  get_client_id(args.project_id, args.location, args.composer_environment)
