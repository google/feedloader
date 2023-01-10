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

"""Define class to handle a Cloud Tasks task."""

import dataclasses
from typing import Any, Mapping


@dataclasses.dataclass()
class InitiatorTask(object):
  """Class to handle a Cloud Tasks task."""

  delete_count: int
  expiring_count: int
  upsert_count: int

  @classmethod
  def from_json(cls, json_data: Mapping[str, Any]) -> 'InitiatorTask':
    """Factory method to create an InitiatorTask from JSON data.

    Args:
      json_data: JSON data sent from Cloud Tasks.

    Returns:
      InitiatorTask object with retrieved information.
    """
    delete_count: int = json_data.get('deleteCount', 0)
    expiring_count: int = json_data.get('expiringCount', 0)
    upsert_count: int = json_data.get('upsertCount', 0)
    return cls(
        delete_count=delete_count,
        expiring_count=expiring_count,
        upsert_count=upsert_count)
