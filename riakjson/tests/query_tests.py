__author__ = 'Dan Kerrigan'

import platform
import time

from riakjson.client import Client
import riakjson.query as query


if platform.python_version() < '2.7':
    unittest = __import__('unittest2')
else:
    import unittest


class FindTests(unittest.TestCase):


    data = [{'_id': age, 'name': name, 'age': age} for age, name in enumerate(['Dan', 'Drew', 'Casey', 'Evan'])]

    def setUp(self):
        self.client = Client()
        self.test_collection = self.client.test_collection

        self.keys = list()

        for record in self.data:
            key = self.test_collection.insert(record)
            #print 'Inserted', key, '->', record
            self.keys.append(key)

        time.sleep(1)

    def tearDown(self):
        for key in self.keys:
            #print 'Deleting key', key
            self.test_collection.delete(key)

        self.keys = list()

    def test_no_match(self):
        print "Test no match"

        q = query.Query(query.eq('name', 'notfound'))

        result = self.test_collection.find(q.build())

        try:

            self.assertEqual(len(result.raw_data), 0, 'Should be 0, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_one(self):
        print "Test find one"

        q = query.Query(query.eq('name', 'Dan'))

        results = self.test_collection.find_one(q.build())

        print "Results", results
        try:
            self.assertEqual(results['name'], 'Dan',
                             'name field in retrieved record did not match, {0}, actually, {1}'.format('Dan',
                                                                                                        results['name']))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, results))

    def test_find(self):
        print "Test find (wildcard)"
        q = query.Query(query.eq('name', 'Dan'))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_and(self):
        print "Test and with 2 terms"
        q = query.Query(query.and_args(query.eq('name', 'Drew'), query.eq('age', 1)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_or(self):
        print "Test or with 2 terms"
        q = query.Query(query.or_args(query.eq('age', 1), query.eq('age', 2)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_range(self):
        print "Test find range"

        q = query.Query(query.between('age', 1, 2))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_sort(self):
        print "Test sorting"

        q = query.Query(query.regex('name', '*'))
        q.order({'name': query.ASCENDING})

        result = self.test_collection.find(q.build())

        try:
            ordered = [record['name'] for record in result.raw_data]

            self.assertEqual(ordered, sorted([record['name'] for record in self.data]),
                             "Result set not ordered, [{0}]".format(', '.join(ordered)))

        except KeyError as e:
            self.fail("Unexpected results, {0}, {1}".format(e, result))

    def test_paging(self):
        print "Test paging"

        q = query.Query(query.regex('name', '*'))
        q.order({'name': query.ASCENDING})
        q.limit(1)

        expected = sorted([record['name'] for record in self.data])
        actual = list()

        try:
            for i, record in enumerate(expected):
                q.offset(i+1)
                result = self.test_collection.find(q.build())
                self.assertEqual(len(result.raw_data), 1, "Only expected 1 result")

                actual.append(result.raw_data[0]['name'])

        except KeyError as e:
            self.fail("Unexpected results, {0}".format(e))

        self.assertEqual(expected, actual, "Result set not ordered, [{0}]".format(', '.join(actual)))

    def test_result_iter(self):
        print "Test iteration on paged result set"

        q = query.Query(query.regex('name', '*'))
        q.limit(1)

        expected_count = len(self.data)

        result = self.test_collection.find(q.build())

        count = 0
        for i, doc in enumerate(result.objects()):
            count += 1
            self.assertIsNotNone(doc)

        self.assertEqual(count, expected_count, "Expected {0} iterations, got {1}".format(expected_count, count))

    def test_stats(self):
        print "Test stats generation"

        q = query.Query(query.regex('name', '*'))
        q.enable_stats('age')

        expected_sum = sum([item['age'] for item in self.data])

        import json
        print json.dumps(q.build())

        result = self.test_collection.find(q.build())

        self.assertTrue('age' in result.stats)

        self.assertEqual(result.stats['age'].sum, expected_sum, "Expected sum {0} but got {1}".format(expected_sum, result.stats['age'].sum))





    def test_find_geo(self):
        pass