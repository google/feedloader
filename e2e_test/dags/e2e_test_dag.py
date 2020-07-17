# coding=utf-8
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

"""Airflow DAG for end to end test."""
import datetime

from airflow import models
from airflow.utils import dates

_DEFAULT_DAG_ARGS = {
    # start_date is 2 days ago because the scheduler triggers soon after
    # start_date + schedule_interval is passed.
    'start_date': dates.days_ago(2)
}

dag = models.DAG(
    dag_id='end_to_end_test',
    schedule_interval=datetime.timedelta(days=1),
    default_args=_DEFAULT_DAG_ARGS)

if __name__ == '__main__':
  dag.cli()
