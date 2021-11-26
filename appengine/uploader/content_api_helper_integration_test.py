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

"""Integration tests for content_api_helper."""

import unittest

import constants
import content_api_helper


class ContentApiHelperTest(unittest.TestCase):

  def test_initialize_api_with_valid_path(self):
    content_api_helper.initialize_api(constants.CONFIG_DIRECTORY,
                                      constants.MC_SERVICE_ACCOUNT_FILE)

  def test_initialize_api_with_invalid_path(self):
    with self.assertRaises(IOError):
      content_api_helper.initialize_api('./wrong_dir',
                                        constants.MC_SERVICE_ACCOUNT_FILE)

  def test_initialize_api_with_invalid_service_account(self):
    with self.assertRaises(IOError):
      content_api_helper.initialize_api(constants.CONFIG_DIRECTORY,
                                        'wrong_file')
