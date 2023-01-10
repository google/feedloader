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

/* Query to get result of the last run.
 *
 * Output example in JSON format:
 * [
 *   {
 *     "channel": "online",
 *     "operation": "delete",
 *     "success_count": "1",
 *     "failure_count": "2",
 *     "skipped_count": "3",
 *   },
 *   {
 *     "channel": "online",
 *     "operation": "upsert",
 *     "success_count": "4",
 *     "failure_count": "5"
 *     "skipped_count": "6",
 *   },
 *   {
 *     "channel": "online",
 *     "operation": "prevent_expiring",
 *     "success_count": "7",
 *     "failure_count": "8"
 *     "skipped_count": "9",
 *   }
 * ]
 */

SELECT
  UniqueRunResults.channel,
  UniqueRunResults.operation,
  SUM(UniqueRunResults.success_count) AS success_count,
  SUM(UniqueRunResults.failure_count) AS failure_count,
  SUM(UniqueRunResults.skipped_count) AS skipped_count
FROM
  (
    SELECT
      ProcessResults.channel AS channel,
      ProcessResults.operation AS operation,
      MAX(ProcessResults.success_count) AS success_count,
      MIN(ProcessResults.failure_count) AS failure_count,
      MIN(ProcessResults.skipped_count) AS skipped_count
    FROM
      `${project_id}.${dataset_id}.${table_id}` AS ProcessResults
    WHERE
      ProcessResults.timestamp = (
        SELECT
          MAX(timestamp)
        FROM
          `${project_id}.${dataset_id}.${table_id}`
      )
    GROUP BY
      ProcessResults.channel, ProcessResults.operation, ProcessResults.batch_id
  ) AS UniqueRunResults
GROUP BY
  UniqueRunResults.channel, UniqueRunResults.operation;
