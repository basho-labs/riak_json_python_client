__author__ = 'Dan Kerrigan'

import platform
import time

from riakjson.client import Client
from riakjson.query import Query, GroupSpec
from riakjson.query import eq, between, and_args, or_args, regex, ASCENDING


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

        q = Query(eq('name', 'notfound'))

        result = self.test_collection.find(q.build())

        try:

            self.assertEqual(len(result.raw_data), 0, 'Should be 0, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_one(self):
        print "Test find one"

        q = Query(eq('name', 'Dan'))

        results = self.test_collection.find_one(q.build())

        try:
            self.assertEqual(results['name'], 'Dan',
                             'name field in retrieved record did not match, {0}, actually, {1}'.format('Dan',
                                                                                                        results['name']))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, results))

    def test_find(self):
        print "Test find (exact match)"
        q = Query(eq('name', 'Dan'))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_limit_0(self):
        print "Test find (limit 0)"
        q = Query(eq('name', 'Dan'))
        q.limit(0)

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 0, 'Should be 0, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_and(self):
        print "Test and with 2 terms"
        q = Query(and_args(eq('name', 'Drew'), eq('age', 1)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_or(self):
        print "Test or with 2 terms"
        q = Query(or_args(eq('age', 1), eq('age', 2)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_range(self):
        print "Test find range"

        q = Query(between('age', 1, 2))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_sort(self):
        print "Test sorting"

        q = Query(regex('name', '.*'))
        q.order({'name': ASCENDING})

        result = self.test_collection.find(q.build())

        try:
            ordered = [record['name'] for record in result.raw_data]

            self.assertEqual(ordered, sorted([record['name'] for record in self.data]),
                             "Result set not ordered, [{0}]".format(', '.join(ordered)))

        except KeyError as e:
            self.fail("Unexpected results, {0}, {1}".format(e, result))

    def test_paging(self):
        print "Test paging"

        q = Query(regex('name', '.*'))
        q.order({'name': ASCENDING})
        q.limit(1)

        expected = sorted([record['name'] for record in self.data])
        actual = list()

        try:
            for i, record in enumerate(expected):
                q.offset(i+1)
                result = self.test_collection.find(q.build())
                self.assertEqual(len(result.raw_data), 1, "Only expected 1 result {0}".format(result.raw_data))

                actual.append(result.raw_data[0]['name'])

        except KeyError as e:
            self.fail("Unexpected results, {0}".format(e))

        self.assertEqual(expected, actual, "Result set not ordered, [{0}]".format(', '.join(actual)))

    def test_result_iter(self):
        print "Test iteration on paged result set"

        q = Query(regex('name', '.*'))
        q.limit(1)

        expected_count = len(self.data)

        result = self.test_collection.find(q.build())

        count = len(list(result.objects()))

        self.assertEqual(count, expected_count, "Expected {0} iterations, got {1}".format(expected_count, count))

    def test_grouping_by_field(self):
        print 'Test grouping by field'

        q = Query(regex('name', '.*'))
        q.add_grouping(GroupSpec(field='name'))

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.groups) > 0)

    def test_grouping_by_query(self):
        print 'Test grouping by query'

        q = Query()
        group_spec = GroupSpec()
        group_spec.add_group_query(or_args(eq('name', 'Dan'), eq('name', 'Drew')))
        group_spec.add_group_query(eq('name', 'Evan'))
        group_spec.limit = 10

        q.add_grouping(group_spec)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.groups) > 0)



    #def test_stats(self):
    #    print "Test stats generation"
    #
    #    q = query.Query(query.regex('name', '.*'))
    #    q.stats_on('age')
    #    q.limit(0)
    #
    #    expected_sum = sum([item['age'] for item in self.data])
    #
    #    result = self.test_collection.find(q.build())
    #
    #    self.assertTrue('age' in result.stats.fields)
    #
    #    self.assertEqual(result.stats.fields['age'].sum, expected_sum,
    #                     "Expected sum {0} but got {1}".format(expected_sum, result.stats.fields['age'].sum))
    #
    #def test_stats_with_facet(self):
    #    print "Test stats with facet generation"
    #
    #    q = query.Query(query.regex('name', '.*'))
    #    q.stats_on('age')
    #    q.stats_args({'facet': 'name'})
    #    q.limit(0)
    #
    #    result = self.test_collection.find(q.build())
    #
    #    self.assertTrue('age' in result.stats.fields)
    #    self.assertTrue('name' in result.stats.fields['age'].facets)
    #
    #
    #def test_facets(self):
    #    print "Test facets generation"
    #
    #    q = query.Query(query.regex('name', '.*'))
    #    q.limit(0)
    #    q.stats_on('age')
    #    q.facet_on('age')
    #
    #    result = self.test_collection.find(q.build())
    #
    #    self.assertTrue(len(result.facets) > 0)

    def test_find_geo(self):
        pass