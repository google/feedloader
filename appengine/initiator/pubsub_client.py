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

"""Google Cloud PubSub client."""

import json
import logging
from typing import Any, Mapping

from google.cloud import exceptions
from google.cloud import pubsub

from models import operation_counts

_KEY_OPERATION = 'operation'
_KEY_SUCCESS_COUNT = 'success_count'
_KEY_FAILURE_COUNT = 'failure_count'
_KEY_SKIPPED_COUNT = 'skipped_count'


def _convert_operation_counts_into_json(
    result: operation_counts.OperationCounts) -> Mapping[str, Any]:
  """Convert ContentApiResult to JSON-encodable dict.

  Args:
    result: A ContentApiResult object.

  Returns:
    JSON-encodable dict to be included in a Pub/Sub message.
  """
  result_dict = {
      _KEY_OPERATION: result.operation,
      _KEY_SUCCESS_COUNT: result.success_count,
      _KEY_FAILURE_COUNT: result.failure_count,
      _KEY_SKIPPED_COUNT: result.skipped_count
  }
  return result_dict


class PubSubClient(object):
  """Client class that manipulates Google Cloud PubSub."""

  def __init__(self, client) -> None:
    """Initialization function.

    Args:
      client: google.cloud.pubsub.PublisherClient object.
    """
    self._client = client

  @classmethod
  def from_service_account_json(cls, service_account_path) -> 'PubSubClient':
    """Factory to retrieve JSON credentials while creating a client.

    Args:
      service_account_path: Path to service account configuration file.

    Returns:
      A client created with the retrieved JSON credentials.
    """
    client = pubsub.PublisherClient.from_service_account_json(
        filename=service_account_path)
    return cls(client)

  def trigger_result_email(
      self, project_id: str, topic_name: str,
      operation_counts_dict: Mapping[str, operation_counts.OperationCounts],
      local_inventory_feed_enabled: bool
  ) -> None:
    """Publishes a message to PubSub to trigger the mailer service.

    Args:
      project_id: The ID of the GCP project
      topic_name: The PubSub topic ID that triggers the mailer
      operation_counts_dict: A mapping of operations to success/failure/skipped
        counts
      local_inventory_feed_enabled: Whether local inventory feeds are enabled
        or not.
    """
    topic = f'projects/{project_id}/topics/{topic_name}'
    message = {
        'attributes': {
            'content_api_results':
                json.dumps(
                    operation_counts_dict,
                    default=_convert_operation_counts_into_json),
            'local_inventory_feed_enabled': local_inventory_feed_enabled,
        }
    }
    try:
      self._client.publish(topic, json.dumps(message).encode('utf-8'))
    except exceptions.GoogleCloudError as cloud_error:
      logging.exception('PubSub to mailer publish failed: %s', cloud_error)
