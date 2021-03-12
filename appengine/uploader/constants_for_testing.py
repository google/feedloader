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

"""Constants used within the project for unit tests."""



# Constants for testing
ITEM_WITH_MPN_AS_SYMBOL_PRE = {'item_id': '1', 'title': 'title1',
                               'description': 'desc1', 'mpn': '-'}
ITEM_WITH_MPN_AS_SYMBOL_POST = {'item_id': '1', 'title': 'title1',
                                'description': 'desc1'}
ITEM_WITH_MPN_AS_SYMBOL_MODIFICATION_COUNT = 1

ITEM_WITH_MPN_AS_SINGLE_DIGIT_PRE = {'item_id': '2', 'title': 'title2',
                                     'description': 'desc2', 'mpn': '0'}
ITEM_WITH_MPN_AS_SINGLE_DIGIT_POST = {'item_id': '2', 'title': 'title2',
                                      'description': 'desc2'}
ITEM_WITH_MPN_AS_SINGLE_DIGIT_MODIFICATION_COUNT = 1

ITEM_WITH_NUMERIC_MPN_PRE = {'item_id': '1', 'title': 'title1',
                             'description': 'desc1', 'mpn': '12345'}
ITEM_WITH_NUMERIC_MPN_POST = {'item_id': '1', 'title': 'title1',
                              'description': 'desc1', 'mpn': '12345'}
ITEM_WITH_NUMERIC_MPN_MODIFICATION_COUNT = 0

ITEM_WITH_COMPLEX_MPN_PRE = {'item_id': '2', 'title': 'title2',
                             'description': 'desc2', 'mpn': 'AT-01'}
ITEM_WITH_COMPLEX_MPN_POST = {'item_id': '2', 'title': 'title2',
                              'description': 'desc2', 'mpn': 'AT-01'}
ITEM_WITH_COMPLEX_MPN_MODIFICATION_COUNT = 0

ITEM_WITH_NO_MPN_PRE = {'item_id': '1', 'title': 'title1',
                        'description': 'desc1'}
ITEM_WITH_NO_MPN_POST = {'item_id': '1', 'title': 'title1',
                         'description': 'desc1'}
ITEM_WITH_NO_MPN_MODIFICATION_COUNT = 0
