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

"""Define class to handle a Cloud Tasks task."""

import dataclasses
from typing import Any, Mapping


@dataclasses.dataclass()
class UploadTask(object):
  """Class to handle a Cloud Tasks task."""

  start_index: int
  batch_size: int
  timestamp: str

  @classmethod
  def from_json(cls, json_data: Mapping[str, Any]) -> 'UploadTask':
    """Factory method to create UploadTask from JSON data.

    Args:
      json_data: JSON data sent from Cloud Tasks.

    Returns:
      UploadTask object with retrieved information.
    """
    start_index: int = json_data.get('start_index', 0)
    batch_size: int = json_data.get('batch_size', 0)
    timestamp: str = json_data.get('timestamp', '0')
    return cls(
        start_index=start_index, batch_size=batch_size, timestamp=timestamp)
