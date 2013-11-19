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

    def tearDown(self):
        self.test_collection.delete_schema()

    def test_schema_not_found(self):
        print "\nTest schema not found\n"
        self.not_found = self.client.not_found

        expected = [{}]
        response = self.not_found.get_schema('not_found')
        self.assertEqual(expected, response, "Not found schema was found!")

    def test_create_schema(self):
        print '\nTest create schema\n'

        response = self.test_collection.set_schema(self.default_schema, TEST_SCHEMA_NAME)

        self.assertTrue(response, "Could not set schema!")


    def test_get_schema(self):
        print '\nTest get schema\n'

        create_response = self.test_collection.set_schema(self.default_schema, TEST_SCHEMA_NAME)
        self.assertTrue(create_response, "Couldn't create schema for get_schema test")
        response = self.test_collection.get_schema(TEST_SCHEMA_NAME)

        self.assertEqual(self.default_schema, response,
                         "Retrieved schema was not identical to set schema, {0}".format(response))



