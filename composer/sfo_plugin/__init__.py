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

"""Custome plugins for SFO (Shopping Feed Optimizer)."""

from airflow.plugins_manager import AirflowPlugin

from sfo_plugin.operators import bq_to_pubsub_operator
from sfo_plugin.operators import clean_up_operator
from sfo_plugin.operators import wait_for_completion_operator


class SFOPlugin(AirflowPlugin):
  """Custom plugins for SFO."""
  name = 'sfo_plugin'
  operators = [
      clean_up_operator.CleanUpOperator,
      bq_to_pubsub_operator.GetRunResultsAndTriggerReportingOperator,
      wait_for_completion_operator.WaitForCompletionOperator
  ]
