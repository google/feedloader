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

"""Unit tests for utils."""
import os
import unittest
import unittest.mock as mock

import utils

_DUMMY_KEY = 'DUMMY_ENV'
_DUMMY_VALUE = 'dummy_value'


class UtilsTest(unittest.TestCase):

  @mock.patch.dict(os.environ, {_DUMMY_KEY: _DUMMY_VALUE})
  def test_load_environment_variable_returns_correct_value_of_environment_variable(
      self):
    value = utils.load_environment_variable(_DUMMY_KEY)

    self.assertEqual(_DUMMY_VALUE, value)

  def test_load_environment_variable_returns_empty_string_for_non_existing_environment_variable(
      self):
    value = utils.load_environment_variable('NON_EXISTING_KEY')

    self.assertEqual('', value)
