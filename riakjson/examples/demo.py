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

import json
import time
from collections import OrderedDict

from riakjson.client import Client
from riakjson.schema import Schema
from riakjson.query import eq, and_args, or_args, regex, gte, lte
from riakjson.query import Query, GroupSpec, CategorizeSpec, RangeSpec, StatsSpec

HOST = '127.0.0.1'
PORT = 8098

VARS = [('Harold', 33),
        ('Petunia', 31),
        ('Max', 2),
        ('Carrie', 28),
        ('Wilt', 28),
        ('Roberta', 2),
        ('Rowena', 2),
        ('Robert', 40),
        ('Casey', 9000),
        ('Drew', 1),
        ('Dan', 2),
        ('Felix', 3)]

DATA = []
for data in VARS:
    DATA.append(OrderedDict(zip(['name', 'metric'], data)))

RJ_SUFFIX = 'RJIndex'
TEST_COLLECTION = 'demo_collection'

curl_cmds = []

def curl(verb, resource, data=None, base='document', collection=TEST_COLLECTION, accept_header=False):
    uri = "http://{host}:{port}/{base}/{collection}/{resource}".format(host=HOST,
                                                                       port=PORT,
                                                                       base=base + '/collection',
                                                                       collection=collection,
                                                                       resource=resource)
    cmd_fmt = 'curl -v '
    if verb == 'PUT' or verb == 'POST':
        cmd_fmt += '-H"content-type: application/json" '
    if accept_header:
        cmd_fmt += '-H"accept: application/json" '
    cmd_fmt += '-X{verb} {url}'
    cmd = cmd_fmt.format(verb=verb, url=uri)
    if data:
        cmd += ' -d {data}'.format(data=json.dumps(data).replace('"', '\"').replace('$', '\\$'))
    if accept_header:
        cmd += ' | python -m json.tool'

    curl_cmds.append(cmd)

    return cmd


def json_pprint(obj):
    print json.dumps(obj, indent=4, separators=(',', ': '))

def query_pprint(obj):
    json_pprint(obj)
    #pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(dict(obj))

demo_client = Client()
demo_collection = demo_client[TEST_COLLECTION]

def _delete_all_records():
    q = Query(regex('name', '.*'))
    result = demo_collection.find(q.build())
    for obj in result.objects():
        del_result = demo_collection.delete(obj['_id'])
        print obj['_id'], del_result

def set_schema():
    print '\nSetting a schema:\n'

    schema = Schema()
    schema.string('name')
    schema.integer('metric')

    json_pprint(schema.build())

    demo_collection.set_schema(schema.build())

    print '\n' + curl('PUT', 'schema', data=json.dumps(schema.build())) + '\n'


def retrieve_schema():
    print '\nGetting a schema:'

    print '\n' + curl('GET', 'schema') + '\n'

    result = demo_collection.get_schema()

    json_pprint(result)

def insert_records(data):
    for record in data:
        insert_record(record)

def insert_record(record):
    print '\nInsert a Record:'
    json_pprint(record)

    print '\n' + curl('PUT', record['name'], json.dumps(record))

    response = demo_collection.insert(record, record['name'])

    print 'Key:', response

def retrieve_record(record):
    print '\nRetrieve a Record for key, {key}:'.format(key=record['name'])

    print '\n' + curl('GET', record['name'], accept_header=True) + '\n'

    response = json.loads(demo_collection.get(record['name']))

    json_pprint(response)

def delete_record(record):
    print '\nDelete a Record for key, {key}'.format(key=record['name'])

    print '\n' + curl('DELETE', record['name'])

def equality_query(field, value):
    print '\nEquality Query:'
    q = Query(eq(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)


def find_one_equality(field, value):
    print '\nFind one record, Equality Query:'
    q = Query(eq(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/one', data=query, accept_header=True)

    result = demo_collection.find_one(q.build())

    json_pprint(result)


def find_regex(field, value):
    print '\n Query with regex, {regex}'.format(regex=value)

    q = Query(regex(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def find_and(field, value, low_field, low_value, high_field, high_value):
    print '\n Query with boolean and'

    q = Query(and_args(regex(field, value), gte(low_field, low_value), lte(high_field, high_value)))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def find_or(field, value1, value2, value3):
    print '\n Query with boolean or'

    q = Query(or_args(eq(field, value1), eq(field, value2), eq(field, value3)))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def group_by_field(query_field, field):
    print '\nGroup by value for field, {field}'.format(field=field)

    q = Query(regex(query_field, '.*'))

    group = GroupSpec(field=field, limit=10, sort={'metric': 'asc'})

    q.add_grouping(group)

    query_pprint(q.build())

    query = json.dumps(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def group_by_queries(field, regex1, regex2):
    print '\nGroup by query for field, {field}'.format(field=field)

    q = Query(regex(field, '.*'))

    group = GroupSpec(sort={'metric': 'asc'})

    group.add_group_query(regex(field, regex1))
    group.add_group_query(regex(field, regex2))

    q.add_grouping(group)

    query_pprint(q.build())

    query = json.dumps(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def categorize_by_field(query_field, field):
    print '\nCategorize by value for field, {field}'.format(field=field)

    q = Query(regex(query_field, '.*'))
    q.limit(0)

    category = CategorizeSpec()
    category.field = field

    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def categorize_by_range(query_field, field, start, end, increment):
    print '\nCategorize by range for field, {field}'.format(field=field)

    q = Query(regex(query_field, '.*'))
    q.limit(0)

    category = CategorizeSpec()

    _range = RangeSpec(field, start, end, increment)
    category.range_spec = _range

    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def categorize_by_query(field, regex1, regex2):
    print '\nCategorize by range for field, {field}'.format(field=field)

    q = Query(regex(field, '.*'))
    q.limit(0)

    category = CategorizeSpec()

    category.add_categorize_query(regex(field, regex1))
    category.add_categorize_query(regex(field, regex2))

    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def categorize_with_stats(field, stat_field):
    print '\nCategorize with stats on field, {stat_field}'.format(stat_field=stat_field)

    q = Query(regex(field, '.*'))
    q.limit(0)

    category = CategorizeSpec()
    category.stats = StatsSpec(field, stat_field)

    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query, accept_header=True)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)


if __name__ == '__main__':
    set_schema()
    retrieve_schema()

    insert_record(DATA[0])
    retrieve_record(DATA[0])
    delete_record(DATA[0])

    insert_records(DATA)

    time.sleep(1) # a sleep to ensure everything's in place

    find_regex('name', 'R.*')
    equality_query('name', 'Harold')
    find_one_equality('name', 'Harold')
    find_and('name', 'R.*', 'metric', 5, 'metric', 100)
    find_or('metric', 2, 28, 40)
    group_by_field('name', 'metric')
    #group_by_queries('name', 'R.*', '.*a')
    categorize_by_field('name', 'metric')
    categorize_by_range('name', 'metric', 1, 50, 10)
    categorize_by_query('name', 'R.*', '.*a')

    categorize_with_stats('name', 'metric')

    #for curl_cmd in curl_cmds:
    #    print curl_cmd
