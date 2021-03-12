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

"""This module includes components that handle Google Cloud Tasks."""

from google.cloud import tasks


class TasksClient(object):
  """Client class for Google Cloud Tasks."""

  def __init__(self, client: tasks.CloudTasksClient) -> None:
    """Initializes TasksClient.

    Args:
      client: A tasks.CloudTasksClient object.
    """
    self._client = client

  @classmethod
  def from_service_account_file(cls,
                                service_account_path: str) -> 'TasksClient':
    """Factory to retrieve JSON credentials while creating client.

    Args:
      service_account_path: Path to service account configuration file.

    Returns:
      The client created with the retrieved JSON credentials.
    """
    client = tasks.CloudTasksClient.from_service_account_file(
        service_account_path)
    return cls(client)

  def is_queue_empty(self, project: str, location: str,
                     queue_name: str) -> bool:
    """Checks if a queue is empty or not.

    Args:
      project: Project id.
      location: Location of the queue.
      queue_name: Name of the queue.

    Returns:
      Whether a queue is empty or not.
    """
    parent = self._client.queue_path(project, location, queue_name)
    task_list = self._client.list_tasks(parent)
    return not task_list
