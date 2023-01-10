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

"""SQL query constants used in bq-stage-changes (calculate_product_changes)."""

# This query selects the latest unique date from the streaming_items table.
LATEST_DATE_SUBQUERY = '''
  SELECT
    DISTINCT(import_datetime)
  FROM
    $bq_dataset.streaming_items
  ORDER BY
    import_datetime DESC
  LIMIT 1
'''

# This query copies IDs from the "items" table and adds a column for the
# item data's hashed content as well as a single timestamp for all items in
# this feed import.
COPY_ITEM_BATCH_QUERY = '''
  SELECT
    DISTINCT(item_id),
    $mc_column
    TO_BASE64(SHA1(CONCAT($columns_to_hash)))
      AS hashed_content,
    CURRENT_DATETIME() AS import_datetime
  FROM
    $bq_dataset.items AS Items
'''

# This query determines which items no longer exist compared to the last
# batch of imported items, and selects their IDs.
CALCULATE_ITEMS_FOR_DELETION_QUERY = '''
  SELECT
    PreviousRun.item_id,
    PreviousRun.google_merchant_id
  FROM
    (
      SELECT
        item_id,
        google_merchant_id
      FROM
        $bq_dataset.streaming_items
      WHERE
        import_datetime = ($latest_date_subquery)
    ) AS CurrentRun
  RIGHT OUTER JOIN
    (
      SELECT
        item_id,
        google_merchant_id
      FROM
        $bq_dataset.streaming_items
      WHERE
        import_datetime = ($latest_date_subquery OFFSET 1)
    ) AS PreviousRun
  ON
    CurrentRun.item_id = PreviousRun.item_id
  WHERE
    CurrentRun.item_id IS NULL
'''

# This query counts how many items were staged for deletion and this number
# will be passed onto GAE via PubSub.
COUNT_DELETES_QUERY = '''
  SELECT COUNT(*) FROM $bq_dataset.items_to_delete
'''

# This query determines if any items were changed in the current batch
# compared to the previous batch, and selects only the changed items' IDs.
CALCULATE_ITEMS_FOR_UPDATE_QUERY = '''
  SELECT
    Items.item_id
  FROM
    $bq_dataset.items AS Items
  INNER JOIN
    $bq_dataset.streaming_items AS Streaming
    ON
      Items.item_id = Streaming.item_id
    WHERE
      Streaming.import_datetime = (
        SELECT DISTINCT
          import_datetime
        FROM
          $bq_dataset.streaming_items
        ORDER BY
          import_datetime DESC
        LIMIT 1
        OFFSET 1
      )
      AND (
        Streaming.hashed_content != TO_BASE64(SHA1(CONCAT($columns_to_hash)))
      )
'''

# This query determines which items have not been seen compared to the last
# batch of imported items, and selects their IDs.
CALCULATE_ITEMS_FOR_INSERTION_QUERY = '''
  SELECT
      CurrentRun.item_id
    FROM
      (
        SELECT
          item_id
        FROM
          $bq_dataset.streaming_items
        WHERE
          import_datetime = ($latest_date_subquery OFFSET 1)
      ) AS PreviousRun
    RIGHT OUTER JOIN
      (
        SELECT
          item_id
        FROM
          $bq_dataset.streaming_items
        WHERE
          import_datetime = ($latest_date_subquery)
      ) AS CurrentRun
    ON
      CurrentRun.item_id = PreviousRun.item_id
    WHERE
      PreviousRun.item_id IS NULL
'''

# This query counts how many items were staged for update/insert and this
# number will be passed onto GAE via PubSub.
COUNT_UPSERTS_QUERY = '''
  SELECT COUNT(*) FROM $bq_dataset.items_to_upsert
'''

# This query filters the items_expiration_tracking for aging items
# (items over the specified number of days threshold), and is used to
# materialize to a new table for items that need to be resent to Content API
# to prevent them from expiring. It excludes IDs that are already in the
# items_to_upsert table, because items in items_to_upsert will be sent to
# Content API and have their expiration updated anyway.
GET_EXPIRING_ITEMS_QUERY = '''
  SELECT
    E.item_id
  FROM
    $bq_dataset.items_expiration_tracking AS E
    INNER JOIN
    $bq_dataset.items AS I
    USING(item_id)
  WHERE
    DATE_DIFF(CURRENT_DATE('$timezone_utc_offset'), E.last_touched_date, DAY)
    >= $expiration_threshold
    AND E.item_id NOT IN (SELECT item_id FROM $bq_dataset.items_to_upsert)
'''

# This query counts how many items were staged for expiration prevention and
# this number will be passed onto GAE via PubSub.
COUNT_EXPIRING_QUERY = '''
  SELECT COUNT(*) FROM $bq_dataset.items_to_prevent_expiring
'''

# This query deletes the latest imported run's items for the purpose of undoing
# the latest import in the case where upsert threshold is crossed.
DELETE_LATEST_STREAMING_ITEMS = '''
  DELETE FROM $bq_dataset.streaming_items
  WHERE
    import_datetime = ($latest_date_subquery)
'''
