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
"""Airflow operator waiting for the process completion by checking if the Cloud Tasks queue is empty."""

import logging
import time

import airflow
from airflow import models
from google.api_core import exceptions
from google.cloud import tasks

_SECONDS_TO_WAIT = 60


class WaitForCompletionOperator(models.BaseOperator):
  """Airflow operator waiting for the process completion by checking if the Cloud Tasks queue is empty."""

  def __init__(self, project_id, queue_location, queue_name,
               service_account_path, try_count_limit, *args, **kwargs):
    """Inits WaitForCompletionOperator.

    Args:
      project_id: string, GCP project ID.
      queue_location: string, the location of Cloud Tasks queue.
      queue_name: string, the name of Cloud Tasks queue.
      service_account_path: string, file path to the service account.
      try_count_limit: integer, maximum number of times checking the status of
        Cloud Tasks.
      *args: arguments to initialize the super class.
      **kwargs: keyword arguments to initialize the super class.
    """
    super(WaitForCompletionOperator, self).__init__(*args, **kwargs)
    self._project_id = project_id
    self._queue_location = queue_location
    self._queue_name = queue_name
    self._service_acount_path = service_account_path
    self._try_count_limit = try_count_limit

  def execute(self, context):
    """Executes operator.

    This method is invoked by Airflow to wait for the process completion by
    checking the status of Cloud Tasks queue.

    Args:
      context: Airflow context that contains references to related objects to
        the task instance.
    Exceptions:
      airflow.AirflowException: Raised when Cloud Tasks API call failed or try
        count exceeds the limit. It stops Airflow workflow.
    """
    tasks_client = tasks.CloudTasksClient.from_service_account_json(
        self._service_acount_path)
    parent = tasks_client.queue_path(
        project=self._project_id,
        location=self._queue_location,
        queue=self._queue_name)
    for _ in range(self._try_count_limit):
      try:
        task_list = list(tasks_client.list_tasks(parent))
      except (exceptions.GoogleAPICallError,
              exceptions.RetryError) as api_error:
        raise airflow.AirflowException(
            'Cloud Tasks API called failed') from api_error
      except ValueError as value_error:
        raise airflow.AirflowException(
            'Cloud Tasks API call has invalid parameters') from value_error
      if not task_list:
        # This task is done. Move to the next task.
        logging.debug('Queue is empty. Moving to the next task')
        return
      logging.debug('Waiting %s seconds for the next try', _SECONDS_TO_WAIT)
      time.sleep(_SECONDS_TO_WAIT)
    logging.error('The number of try exceeded the limit')
    raise airflow.AirflowException(
        'The number of try exceeded the limit. Stopping Airflow workflow')
