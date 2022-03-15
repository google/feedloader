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

"""Define class to save the result of Content API call."""
import attr


@attr.s
class RunResult(object):
  """Class to save the result of Content API call."""
  channel = attr.ib(default='')
  operation = attr.ib(default='')
  description = attr.ib(default='')
  success_count = attr.ib(default=0)
  failure_count = attr.ib(default=0)
  skipped_count = attr.ib(default=0)

  @classmethod
  def from_dict(cls, result_dict):
    """Factory method to create RunResult from a dict.

    Args:
      result_dict: dict including the result of a run.

    Returns:
      An instance of RunResult generated from the input dict.
    """
    channel = result_dict.get('channel', '')
    operation = result_dict.get('operation', '')
    description = result_dict.get('description', '')
    success_count = result_dict.get('success_count', 0)
    failure_count = result_dict.get('failure_count', 0)
    skipped_count = result_dict.get('skipped_count', 0)
    return cls(channel, operation, description, success_count, failure_count,
               skipped_count)

  def get_total_count(self):
    return self.success_count + self.failure_count + self.skipped_count
