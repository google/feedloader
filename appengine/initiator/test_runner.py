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

"""App Engine local test runner."""

import argparse
import os
import sys
import unittest
import unittest.mock

import google.cloud.logging


def main(test_path, test_pattern):
  # Discover and run tests.
  suite = unittest.loader.TestLoader().discover(test_path, test_pattern)
  return unittest.TextTestRunner(verbosity=2).run(suite)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument(
      '--test-path',
      help='The path to look for tests, defaults to the current directory.',
      default=os.getcwd())
  parser.add_argument(
      '--test-pattern',
      help='The file pattern for test modules, defaults to *_test.py.',
      default='*_test.py')
  args = parser.parse_args()
  google.cloud.logging = unittest.mock.MagicMock()
  result = main(args.test_path, args.test_pattern)
  if not result.wasSuccessful():
    sys.exit(1)
