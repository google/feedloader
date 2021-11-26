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

/* This query updates or inserts today's date into the expiration tracking table for each item that
 * was processed in this run. This table will be used for detecting during each run which items are
 * about to expire.
 * Parameters in this query are replaced by actual values passed by its user.
 * Parameters:
 *  timezone: The number of hours to offset from UTC time.
 *  feed_dataset_id: The dataset id of feed dataset.
 *  expiration_tracking_table_id: The table id of expiration tracking table.
 *  monitor_dataset_id: The dataset id of monitor dataset.
 *  item_results_table_id: The table id of item results table.
 */
DECLARE TIMEZONE STRING DEFAULT '{{ params.timezone }}';


MERGE INTO
  {{ params.feed_dataset_id }}.{{ params.expiration_tracking_table_id }} AS ET
USING
  (
    SELECT DISTINCT
      IR.item_id AS item_id
    FROM
      {{ params.monitor_dataset_id }}.{{ params.item_results_table_id }} AS IR
    WHERE
      IR.timestamp =
        (
          SELECT
            MAX(timestamp)
          FROM
            {{ params.monitor_dataset_id }}.{{ params.item_results_table_id }}
        )
      AND IR.result = 'success'
      AND IR.operation IN ('upsert', 'prevent_expiring')
  ) AS SR
  ON ET.item_id = SR.item_id
WHEN MATCHED THEN
  UPDATE
    SET
      ET.last_touched_date = CURRENT_DATE(TIMEZONE)
WHEN NOT MATCHED BY TARGET THEN
  INSERT
    (item_id, last_touched_date)
  VALUES
    (SR.item_id, CURRENT_DATE(TIMEZONE));
