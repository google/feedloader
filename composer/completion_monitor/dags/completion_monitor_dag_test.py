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

"""Tests for completion_monitor_dag."""

import unittest

from airflow import models

_DAG_ID = 'completion_monitor'
_CLEAN_UP_TASK_ID = 'test_clean_up_task'
_WAIT_FOR_COMPLETION_TASK_ID = 'wait_for_completion'
_BQ_TO_PUBSUB_TASK_ID = 'send_result_from_bigquery_to_pubsub'
_PROJECT_ID = 'test-project'
_QUEUE_LOCATION = 'test-location'
_QUEUE_NAME = 'test-queue'
_TRY_COUNT_LIMIT = 1
_MONITOR_DATASET_ID = 'test_monitor_dataset'
_MONITOR_TABLE_ID = 'test_monitor_table'
_QUERY_FILE_PATH = 'queries/dummy_query.sql'
_TOPIC_NAME = 'test-topic'
_TIMEZONE_UTC_OFFSET = '+00:00'
_FEED_DATASET_ID = 'test_feed_dataset'
_ITEMS_TABLE_ID = 'test_items_table'
_EXPIRATION_TRACKING_TABLE_ID = 'test_expiration_tracking_table'
_ITEM_RESULTS_TABLE_ID = 'test_item_results'
_LOCK_BUCKET = 'test_lock_bucket'


class CompletionMonitorDagTest(unittest.TestCase):

  def setUp(self):
    super(CompletionMonitorDagTest, self).setUp()
    models.Variable.set('DAG_ID', _DAG_ID)
    models.Variable.set('GCP_PROJECT_ID', _PROJECT_ID)
    models.Variable.set('QUEUE_LOCATION', _QUEUE_LOCATION)
    models.Variable.set('QUEUE_NAME', _QUEUE_NAME)
    models.Variable.set('TRY_COUNT_LIMIT', _TRY_COUNT_LIMIT)
    models.Variable.set('MONITOR_DATASET_ID', _MONITOR_DATASET_ID)
    models.Variable.set('MONITOR_TABLE_ID', _MONITOR_TABLE_ID)
    models.Variable.set('LAST_PROCESS_RESULT_QUERY_FILE_PATH', _QUERY_FILE_PATH)
    models.Variable.set('DESTINATION_PUBSUB_TOPIC', _TOPIC_NAME)
    models.Variable.set('TIMEZONE_UTC_OFFSET', _TIMEZONE_UTC_OFFSET)
    models.Variable.set('FEED_DATASET_ID', _FEED_DATASET_ID)
    models.Variable.set('ITEMS_TABLE_ID', _ITEMS_TABLE_ID)
    models.Variable.set('EXPIRATION_TRACKING_TABLE_ID',
                        _EXPIRATION_TRACKING_TABLE_ID)
    models.Variable.set('ITEM_RESULTS_TABLE_ID', _ITEM_RESULTS_TABLE_ID)
    models.Variable.set('LOCK_BUCKET', _LOCK_BUCKET)
    self.dag_bag = models.DagBag(dag_folder='./')
    self.dag = self.dag_bag.dags.get(_DAG_ID)

  def test_import_dags(self):
    self.assertEqual(0, len(self.dag_bag.import_errors))

  def test_project_id_in_dag(self):
    self.assertEqual(_PROJECT_ID, self.dag.default_args.get('project_id'))


if __name__ == '__main__':
  unittest.main()
