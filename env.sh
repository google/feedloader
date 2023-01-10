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

#!/bin/bash
# Set Environment Variables to install the Shopping Feed Optimizer.
# Please change the values for your project.
ALERT_EMAILS=[Comma-separated Email Addresses to receive alerts] # e.g. person1@domain.com, person2@domain.com
ARCHIVE_BUCKET=[ARCHIVE BUCKET] # e.g. archive
BUILD_NOTIFICATION_EMAILS=[Comma-separated Email Addresses to receive completion emails when the App Engine build completes. Leave empty to send no emails.] # e.g. First Last <person1@domain.com>, First Last <person2@domain.com>
COMPLETED_FILES_BUCKET=[COMPLETED FILES BUCKET] # e.g. completed
DELETES_THRESHOLD=[Threshold number of items that would be deleted to alert and abort processing]
EXPIRATION_THRESHOLD=[The number of days that can elapse before an unchanged item will be resent to Content API to prevent it from expiring. Unchanged items will expire in 30 days if not resent to Content API.] # e.g. 25.
FEED_BUCKET=[FEED BUCKET] # e.g. feed
FINISH_EMAILS=[Comma-separated Email Addresses to receive completion emails] # e.g. First Last <person1@domain.com>, First Last <person2@domain.com>
GCP_PROJECT=[PROJECT ID]
IS_MCA=[Whether or not the Merchant Id below is an MCA. Must be either True or False] # e.g. True
USE_LOCAL_INVENTORY_ADS=False [Whether or not to include the "local" destination for Local Inventory Ads]
LOCK_BUCKET=[Name of the bucket to store the calculateProductChanges EOF lock file] # e.g. lock
MERCHANT_ID=[ID of the Merchant Center Account ID to upload items to, or the MCA parent Id if an MCA.]
MONITOR_BUCKET=[MONITOR BUCKET] # e.g. monitor
RETRIGGER_BUCKET=[RETRIGGER BUCKET] # e.g. retrigger
SHOPTIMIZER_API_INTEGRATION_ON=[Boolean: Whether Shoptimizer is used or not. Must be either True or False] # e.g. True
SHOPTIMIZER_URL=[The endpoint base URL of a Shoptimizer API deployment, if available. If Shoptimizer API is not used, please provide any non empty string.] # e.g. https://shoptimizer-abcdefghij-kl.a.run.app
SOURCE_REPO=[Name of the Git Cloud Source Repository to create]
TIMEZONE_UTC_OFFSET=[The number of hours to offset from UTC time that represents your timezone, preceded by a + or -] # e.g. +09:00 or -08:00
TRIGGER_COMPLETION_BUCKET=[Bucket name to trigger completion monitor] # e.g. trigger-completion-check
UPDATE_BUCKET=[UPDATE BUCKET] # e.g. update
UPSERTS_THRESHOLD=[Threshold number of items that would be upserted to alert and abort processing]
