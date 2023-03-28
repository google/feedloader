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

"""Constants used within the project."""

import enum
from typing import Any, Dict

# Constants for configuration.
CONFIG_DIRECTORY = './config'
APPLICATION_NAME = 'Shopping Feed'

# Constants for authentication.
GCP_SERVICE_ACCOUNT_FILE = 'gcp_service_account.json'
GCP_SERVICE_ACCOUNT_PATH = CONFIG_DIRECTORY + '/' + GCP_SERVICE_ACCOUNT_FILE
MC_SERVICE_ACCOUNT_FILE = 'mc_service_account.json'

# Constants for Content API.
SERVICE_NAME = 'content'
CONTENT_API_VERSION = 'v2.1'
SANDBOX_SERVICE_VERSION = 'v2sandbox'
CONTENT_API_SCOPE = 'https://www.googleapis.com/auth/' + SERVICE_NAME
CONTENT_LANGUAGE = 'ja'
TARGET_COUNTRY = 'JP'
TARGET_CURRENCY = 'JPY'

DATASET_ID_FOR_PROCESSING = 'processing_feed_data'
DATASET_ID_FOR_MONITORING = 'monitor_data'
TABLE_ID_FOR_RESULT_COUNTS_MONITORING = 'process_result'
TABLE_ID_FOR_ITEM_RESULTS_MONITORING = 'item_results'

# Type aliases
Batch = Dict[str, Any]
BatchIdToItemId = Dict[int, Any]
Product = Dict[str, Any]


# Enums
class Method(enum.Enum):
  """Enum for Content API methods."""
  INSERT = 'insert'
  DELETE = 'delete'


class Operation(enum.Enum):
  """Enum for item operations."""
  UPSERT = 'upsert'
  DELETE = 'delete'
  PREVENT_EXPIRING = 'prevent_expiring'


class FeedType(enum.Enum):
  """Enum for feed types."""

  PRIMARY = 'primary'
  LOCAL = 'local'


class Channel(enum.Enum):
  """Enum for shopping channels."""
  ONLINE = 'online'
  LOCAL = 'local'


@enum.unique
class ShoppingApiErrorCodes(enum.Enum):
  """An Enum class for mapping error values to the underlying error."""
  # HTTP codes returned by the API
  MALFORMED_REQUEST = 400
  AUTHENTICATION_ERROR = 401
  ACCESS_ERROR = 403
  RESOURCE_NOT_FOUND = 404
  SOCKET_TIMEOUT = 408
  DUPLICATE_CHANGES_DETECTED = 409
  QUOTA_LIMITS_REACHED = 429
  INTERNAL_SERVER_ERROR = 500
  OPERATION_NOT_PERMITTED = 501
