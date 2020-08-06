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

"""Helper module that provides functions to initialize an API client."""

import os

import google_auth_httplib2
from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient import http

import constants


def initialize_api(config_path, service_account_file):
  """Initialization of API service and configuration for the Content API.

  Args:
    config_path: string, relative path to a directory where configuration files
      exist.
    service_account_file: string, file name of the service account configuration
      file.

  Returns:
    A service object to interface with the Content API.
  """
  credentials = _authorize(config_path, service_account_file)
  auth_http = google_auth_httplib2.AuthorizedHttp(
      credentials,
      http=http.set_user_agent(http.build_http(), constants.APPLICATION_NAME))
  service = discovery.build(
      constants.SERVICE_NAME,
      constants.CONTENT_API_VERSION,
      http=auth_http,
      cache_discovery=False)
  return service


def _authorize(config_path, service_account_file):
  """Authorization for the Content API.

  Args:
      config_path: string, path to a directory where configuration files exist.
      service_account_file: string, file name of the service account
        configuration file.

  Returns:
      An google.auth.credentials.Credentials object suitable for
      accessing the Content API.
  """
  service_account_path = _get_absolute_path(config_path, service_account_file)
  return service_account.Credentials.from_service_account_file(
      service_account_path, scopes=[constants.CONTENT_API_SCOPE])


def _get_absolute_path(config_directory, config_file):
  """Returns absolute path of the configuration file."""
  base_path = os.path.dirname(os.path.abspath(__file__))
  linked_path = os.path.join(base_path, config_directory, config_file)
  absolute_path = os.path.normpath(linked_path)
  return absolute_path
