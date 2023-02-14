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

/** This query loads items to upsert to Merchant Center. */
SELECT
  LATEST_ITEMS.*
FROM
  (
    SELECT
      ALL_ITEMS.*
    FROM
      `${project_id}.${feed_data_dataset_id}.items` AS ALL_ITEMS
  ) AS LATEST_ITEMS
JOIN
  `${project_id}.${feed_data_dataset_id}.items_to_upsert` AS UPDATE_ITEMS
  USING (item_id);
