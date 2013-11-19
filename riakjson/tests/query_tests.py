__author__ = 'Dan Kerrigan'

import platform
import time

from riakjson.client import Client
from riakjson.query import Query, GroupSpec, CategorizeSpec, RangeSpec, StatsSpec
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
        print "\nTest no match\n"

        q = Query(eq('name', 'notfound'))

        result = self.test_collection.find(q.build())

        try:

            self.assertEqual(len(result.raw_data), 0, 'Should be 0, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_one(self):
        print "\nTest find one\n"

        q = Query(eq('name', 'Dan'))

        results = self.test_collection.find_one(q.build())

        try:
            self.assertEqual(results['name'], 'Dan',
                             'name field in retrieved record did not match, {0}, actually, {1}'.format('Dan',
                                                                                                        results['name']))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, results))

    def test_find(self):
        print "\nTest find (exact match)\n"
        q = Query(eq('name', 'Dan'))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_limit_0(self):
        print "\nTest find (limit 0)\n"
        q = Query(eq('name', 'Dan'))
        q.limit(0)

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 0, 'Should be 0, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_and(self):
        print "\nTest and with 2 terms\n"
        q = Query(and_args(eq('name', 'Drew'), eq('age', 1)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 1, 'Should be 1, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_or(self):
        print "\nTest or with 2 terms\n"
        q = Query(or_args(eq('age', 1), eq('age', 2)))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_find_range(self):
        print "\nTest find range\n"

        q = Query(between('age', 1, 2))

        result = self.test_collection.find(q.build())

        try:
            self.assertEqual(len(result.raw_data), 2, 'Should be 2, actually {0}'.format(len(result.raw_data)))

        except KeyError as e:
            self.fail("Unexpected result, {0}, {1}".format(e, result))

    def test_sort(self):
        print "\nTest sorting\n"

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
        print "\nTest paging\n"

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
        print "\nTest iteration on paged result set\n"

        q = Query(regex('name', '.*'))
        q.limit(1)

        expected_count = len(self.data)

        result = self.test_collection.find(q.build())

        count = len(list(result.objects()))

        self.assertEqual(count, expected_count, "Expected {0} iterations, got {1}".format(expected_count, count))

    def test_grouping_by_field(self):
        print '\nTest grouping by field\n'

        q = Query(regex('name', '.*'))
        q.add_grouping(GroupSpec(field='name'))

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.groups) > 0)

    def test_grouping_by_query(self):
        print '\nTest grouping by query\n'

        q = Query(regex('name', '.*'))
        group_spec = GroupSpec()
        #group_spec.add_group_query(or_args(eq('name', 'Dan'), eq('name', 'Drew')))
        group_spec.add_group_query(eq('name', 'Evan'))
        group_spec.sort = {'name': ASCENDING}
        group_spec.limit = 10

        q.add_grouping(group_spec)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.groups) > 0)

    def test_categorize_by_field(self):
        print '\nTest categorize by field\n'

        q = Query(regex('name', '.*'))

        cat_spec = CategorizeSpec()
        cat_spec.field = 'name'

        q.add_categorization(cat_spec)
        q.limit(0)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.categories) > 0)

    def test_categorize_by_range(self):
        print '\nTest categorize by range'

        q = Query(regex('name', '.*'))

        range_spec = RangeSpec('age')
        range_spec.start = 1
        range_spec.end = 10
        range_spec.increment = 1

        cat_spec = CategorizeSpec()
        cat_spec.range_spec = range_spec

        q.add_categorization(cat_spec)

        q.limit(0)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.categories) > 0)

    def test_categorize_by_query(self):
        q = Query(regex('name', '.*'))

        cat_spec = CategorizeSpec()
        cat_spec.add_categorize_query(eq('name', 'Dan'))

        q.add_categorization(cat_spec)

        q.limit(0)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.categories) > 0)

    def test_categorize_with_stats(self):
        print '\nTest categorize with stats\n'

        q = Query(regex('name', '.*'))

        cat_spec = CategorizeSpec()
        cat_spec.stats = StatsSpec('name', 'age')

        q.add_categorization(cat_spec)
        q.limit(0)

        result = self.test_collection.find(q.build())

        self.assertTrue(len(result.categories) > 0)

    def test_find_geo(self):
        pass