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

"""Module that pushes tasks to Task Queue."""
import json
import logging

from google.api_core import exceptions
from google.cloud import tasks


class TasksClient(object):
  """Client to bundle TaskQueue manipulation.

  Attributes:
    client: tasks.CloudTasksClient object.
    url: URL to push tasks.
    project_id: GCP project ID.
    queue_name: Name of Task Queue queue.
    location: Location of the queue.
  """

  def __init__(self, client, url, project_id, location, queue_name):
    self._client = client
    self._url = url
    self._queue_name = queue_name
    self._parent = tasks.CloudTasksClient.queue_path(project_id, location,
                                                     queue_name)

  @classmethod
  def from_service_account_json(cls, service_account_path, url, project_id,
                                location, queue_name):
    """Factory to retrieve JSON credentials while creating a client.

    Args:
      service_account_path: Path to service account configuration file.
      url: URL to push tasks.
      project_id: GCP project ID.
      location: Location of the queue.
      queue_name: Name of Task Queue queue.

    Returns:
      A client created with the retrieved JSON credentials.
    """
    client = tasks.CloudTasksClient.from_service_account_file(
        service_account_path)
    return cls(
        client=client,
        url=url,
        project_id=project_id,
        location=location,
        queue_name=queue_name)

  def push_tasks(self, total_items, batch_size, timestamp):
    """Create right number of tasks with the batch size.

    Args:
      total_items: Amount of total items to update.
      batch_size: Number of items processed in a task.
      timestamp: String of a time stamp passing to Task Queue.
    """
    start_index = 0
    while start_index < total_items:
      self._push_task(start_index, batch_size, timestamp)
      start_index += batch_size

  def _push_task(self, start_index, batch_size, timestamp):
    """Push a task to Task Queue with the first item's index in the batch.

    Args:
      start_index: Index of the first item in a batch. It is used to load item
        data from BigQuery.
      batch_size: Number of items processed in a task.
      timestamp: String of a time stamp passing to Task Queue.
    """
    payload = json.dumps({
        'start_index': start_index,
        'batch_size': batch_size,
        'timestamp': timestamp
    }).encode()
    task = {
        'app_engine_http_request': {
            'relative_uri': self._url,
            'body': payload
        }
    }
    try:
      self._client.create_task(parent=self._parent, task=task)
    except exceptions.GoogleAPICallError as api_error:
      logging.exception('Failed to create a task: %s', api_error.message)
