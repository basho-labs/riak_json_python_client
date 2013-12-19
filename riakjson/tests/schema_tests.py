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
from riakjson.schema import Schema

if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest

TEST_COLLECTION = 'schema_test_collection'
TEST_SCHEMA_NAME = 'test_schema'

class SchemaTests(unittest.TestCase):

    default_schema = Schema().string('first_name')\
                             .string('last_name')\
                             .multi_string('description')\
                             .number('age')\
                             .geo('location').build()


    def setUp(self):
        self.client = Client()
        self.test_collection = self.client[TEST_COLLECTION]

    def clean_up_schema(self):
        self.test_collection.delete_schema()

    def test_schema_not_found(self):
        print "\nTest schema not found\n"
        self.not_found = self.client.not_found

        expected = [{}]
        response = self.not_found.get_schema()
        self.assertEqual(expected, response, "Not found schema was found!")

    def test_create_schema(self):
        print '\nTest create schema\n'

        response = self.test_collection.set_schema(self.default_schema)

        self.assertTrue(response, "Could not set schema!")

        self.clean_up_schema()

    def test_get_schema(self):
        print '\nTest get schema\n'

        create_response = self.test_collection.set_schema(self.default_schema)
        self.assertTrue(create_response, "Couldn't create schema for get_schema test")
        response = self.test_collection.get_schema()

        self.assertEqual(self.default_schema, response,
                         "Retrieved schema was not identical to set schema, {0}".format(response))

        self.clean_up_schema()

