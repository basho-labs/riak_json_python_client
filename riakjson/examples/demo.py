__author__ = 'dankerrigan'

import json

from riakjson.client import Client
from riakjson.query import eq, and_args, or_args, regex, gte, lte
from riakjson.query import Query, GroupSpec, CategorizeSpec, RangeSpec, StatsSpec

HOST = '127.0.0.1'
PORT = 8098

VARS = [('Harold', 33, 'M', 'WV'),
        ('Petunia', 31, 'F', 'VA'),
        ('Max', 2, 'M', 'MD'),
        ('Carrie', 28, 'F', 'WV'),
        ('Wilt', 28, 'M', 'WV'),
        ('Roberta', 2, 'F', 'WA'),
        ('Rowena', 2, 'f', 'WA'),
        ('Robert', 40, 'M', 'MI')]

DATA = []
for data in VARS:
    DATA.append(dict(zip(['name', 'age', 'gender', 'state'], data)))

RJ_SUFFIX = 'RJIndex'
TEST_COLLECTION = 'demo_collection'

def curl(verb, resource, data=None, base='document', collection=TEST_COLLECTION):
    uri = "http://{host}:{port}/{base}/{collection}/{resource}".format(host=HOST,
                                                                       port=PORT,
                                                                       base=base + '/collection',
                                                                       collection=collection,
                                                                       resource=resource)
    cmd = 'curl -v -H"content-type: application/json" -H"accept: application/json" -X{verb} {url}'.format(verb=verb, url=uri)
    if data:
        cmd += ' -d {data}'.format(data=json.dumps(data).replace('"', '\"'))

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

    print '\n' + curl('GET', record['name']) + '\n'

    response = json.loads(demo_collection.get(record['name']))

    json_pprint(response)

def delete_record(record):
    print 'Delete a Record for key, {key}'.format(key=record['name'])

    print '\n' + curl('DELETE', record['name'])

def equality_query(field, value):
    print '\nEquality Query:'
    q = Query(eq(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)


def find_one_equality(field, value):
    print '\nFind one record, Equality Query:'
    q = Query(eq(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/one', data=query)

    result = demo_collection.find_one(q.build())

    json_pprint(result)


def find_regex(field, value):
    print '\n Query with regex, {regex}'.format(regex=value)

    q = Query(regex(field, value))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def find_and(field, value, low_field, low_value, high_field, high_value):
    print '\n Query with boolean and'

    q = Query(and_args(regex(field, value), gte(low_field, low_value), lte(high_field, high_value)))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def find_or(field, value1, value2, value3):
    print '\n Query with boolean or'

    q = Query(or_args(eq(field, value1), eq(field, value2), eq(field, value3)))

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def group_by_field(field):
    print '\nGroup by value for field, {field}'.format(field=field)

    q = Query(regex(field, '.*'))

    group = GroupSpec()
    group.field = field
    group.limit = 10

    q.add_grouping(group)

    query_pprint(q.build())

    query = json.dumps(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

#def group_by_queries(field, regex1, regex2):
#    print '\nGroup by query for field, {field}'.format(field=field)
#
#    q = Query(regex(field, '.*'))
#
#    group = GroupSpec()
#
#    group.add_group_query(regex(field, regex1))
#    group.add_group_query(regex(field, regex2))
#
#    q.add_grouping(group)
#
#    query_pprint(q.build())
#
#    query = json.dumps(q.build())
#
#    print '\n' + curl('PUT', 'query/all', data=query)
#
#    result = demo_collection.find(q.build())
#
#    json_pprint(result.response_doc)


def categorize_by_field(field):
    print '\nCategorize by value for field, {field}'.format(field=field)

    q = Query(regex(field, '.*'))
    q.limit(0)

    category = CategorizeSpec()
    category.field = field
    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)

def categorize_by_range(field, start, end, increment):
    print '\nCategorize by range for field, {field}'.format(field=field)

    q = Query(regex(field, '.*'))
    q.limit(0)

    category = CategorizeSpec()

    _range = RangeSpec('age', start, end, increment)
    category.range_spec = _range

    q.add_categorization(category)

    query = json.dumps(q.build())

    query_pprint(q.build())

    print '\n' + curl('PUT', 'query/all', data=query)

    result = demo_collection.find(q.build())

    json_pprint(result.response_doc)


def retrieve_inferred_schema():
    print '\nRetrieve inferred schema:'

    schema = '{collection}'.format(collection=TEST_COLLECTION)
    resource = 'schema/' + schema
    print '\n' + curl('GET', resource)

    response = demo_collection.get_schema(schema)
    json_pprint(response)

if __name__ == '__main__':

    #_delete_all_records()

    insert_record(DATA[0])
    retrieve_record(DATA[0])
    delete_record(DATA[0])

    insert_records(DATA)

    equality_query('name', 'Harold')
    find_one_equality('name', 'Harold')
    find_regex('name', 'R.*')
    find_and('name', 'R.*', 'age', 5, 'age', 100)
    find_or('state', 'WV', 'MD', 'VA')
    group_by_field('state')
    #group_by_queries('name', 'R.*', '.*a')
    categorize_by_field('state')
    categorize_by_range('age', 1, 50, 10)



    #retrieve_inferred_schema()
