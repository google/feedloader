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

"""Tests for e2e_test_dag."""
import importlib
import unittest

from airflow import models


class EndToEndTestDagTest(unittest.TestCase):

  def setUp(self):
    super(EndToEndTestDagTest, self).setUp()
    self.module = importlib.import_module('e2e_test_dag')

  def test_has_valid_dag(self):
    self.assertIsInstance(self.module.dag, models.DAG)


if __name__ == '__main__':
  unittest.main()
