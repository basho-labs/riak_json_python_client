## ------------------------------------------------------------------- 
## 
## Copyright (c) "2013" Basho Technologies, Inc.
##
## This file is provided to you under the Apache License,
## Version 2.0 (the "License"); you may not use this file
## except in compliance with the License.  You may obtain
## a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
## -------------------------------------------------------------------

__author__ = 'dankerrigan'

import platform

from riakjson.client import Client

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

TEST_COLLECTION = 'test_collection'


class BasicClientTests(unittest.TestCase):
    key_data = list()

    def setUp(self):
        self.client = Client()
        self.test_collection = self.client[TEST_COLLECTION]

    def test_insert(self):
        data = {'name': 'Keymore Dan'}
        nonrandom_key = self.test_collection.insert(data, 'key')
        self.assertEquals(nonrandom_key, 'key')
        self.key_data.append((nonrandom_key, data))

    def test_insert_no_key(self):
        data = {'name': 'Keyless Dan'}
        random_key = self.test_collection.insert(data)
        self.assertIsNotNone(random_key)
        self.key_data.append((random_key, data))

    def test_get(self):
        for key, data in self.key_data:
            doc = self.test_collection.get(key)
            self.assertEqual(data, doc)

    def test_delete(self):
        for key, data in self.key_data:
            self.assertTrue(self.test_collection.delete(key))

        self.key_data = list()