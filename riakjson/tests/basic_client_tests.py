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