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

"""Google Cloud Storage client."""
import logging

from google.cloud import exceptions
from google.cloud import storage


class StorageClient(object):
  """Client class that manipulates Google Cloud Storage."""

  def __init__(self, client: storage.Client, bucket: storage.Client) -> None:
    """Initialization function.

    Args:
      client: google.cloud.storage.Client object.
      bucket: google.cloud.storage.Bucket object.
    """
    self._client = client
    self._bucket = bucket

  @classmethod
  def from_service_account_json(cls, service_account_path: str,
                                bucket_name: str) -> 'StorageClient':
    """Factory to retrieve JSON credentials while creating a client.

    Args:
      service_account_path: Path to service account configuration file.
      bucket_name: String, name of Cloud Storage bucket.

    Returns:
      A client created with the retrieved JSON credentials.
    """
    client = storage.Client.from_service_account_json(
        json_credentials_path=service_account_path)
    clean_bucket_name = _retrieve_bucket_name(bucket_name)
    bucket = client.get_bucket(clean_bucket_name)
    return cls(client=client, bucket=bucket)

  def upload_eof(self):
    """Upload EOF file to a bucket."""
    blob = self._bucket.blob('EOF')
    try:
      blob.upload_from_string('')
      logging.info('EOF file successfully uploaded to bucket %s',
                   self._bucket.id)
    except exceptions.GoogleCloudError as cloud_error:
      logging.exception('EOF file failed to be uploaded: %s', cloud_error)

  def delete_eof_lock(self):
    """Delete EOF.lock file from a bucket."""
    blob = self._bucket.blob('EOF.lock')
    try:
      blob.delete()
      logging.info('EOF.lock file successfully deleted from bucket %s',
                   self._bucket.id)
    except exceptions.GoogleCloudError as cloud_error:
      logging.exception('EOF.lock file failed to be deleted: %s', cloud_error)


def _retrieve_bucket_name(bucket_url_or_name: str) -> str:
  """Returns bucket name retrieved from URL.

  Args:
    bucket_url_or_name: The name or url of the storage bucket.
  """
  return bucket_url_or_name.split('/')[-1]
