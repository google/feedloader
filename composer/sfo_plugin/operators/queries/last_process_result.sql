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

/* Query to get result of the last run.
 *
 * Output example in JSON format:
 * [
 *   {
 *     "operation": "delete",
 *     "success_count": "1",
 *     "failure_count": "2",
 *     "skipped_count": "3",
 *   },
 *   {
 *     "operation": "upsert",
 *     "success_count": "4",
 *     "failure_count": "5"
 *     "skipped_count": "6",
 *   },
 *   {
 *     "operation": "prevent_expiring",
 *     "success_count": "7",
 *     "failure_count": "8"
 *     "skipped_count": "9",
 *   }
 * ]
 */
SELECT
  DEDUPLICATED_RUN_RESULTS.operation,
  SUM(DEDUPLICATED_RUN_RESULTS.success_count) AS success_count,
  SUM(DEDUPLICATED_RUN_RESULTS.failure_count) AS failure_count,
  SUM(DEDUPLICATED_RUN_RESULTS.skipped_count) AS skipped_count
FROM
  (
    SELECT
      PROCESS_RESULT.operation AS operation,
      MAX(PROCESS_RESULT.success_count) AS success_count,
      MIN(PROCESS_RESULT.failure_count) AS failure_count,
      MIN(PROCESS_RESULT.skipped_count) AS skipped_count
    FROM
      `${project_id}.${dataset_id}.${table_id}` AS PROCESS_RESULT
    WHERE
      PROCESS_RESULT.timestamp =
        (
          SELECT
             MAX(timestamp)
           FROM
             `${project_id}.${dataset_id}.${table_id}`
        )
    GROUP BY
      PROCESS_RESULT.operation, PROCESS_RESULT.batch_id
  ) AS DEDUPLICATED_RUN_RESULTS
GROUP BY
  DEDUPLICATED_RUN_RESULTS.operation;
