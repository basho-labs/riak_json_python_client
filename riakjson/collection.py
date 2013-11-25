__author__ = 'dankerrigan'

import json
from result import Result

COLLECTION_RESOURCE = 'collection'
YZ_RESOURCE = 'yz/'
INTERNAL_INDEX_SUFFIX = 'RJIndex'
DEFAULT_HEADERS = {'content-type': 'application/json', 'accept': 'application/json'}

DEBUG = False

class Collection(object):

    def __init__(self, name, connection):
        self.name = name
        self.connection = connection

    def get(self, key):
        resource = '{base}/{collection}/{key}'.format(base=COLLECTION_RESOURCE,
                                                      collection=self.name,
                                                      key=key)

        code, headers, data = self.connection.get(resource, DEFAULT_HEADERS)

        if code == 200:
            return data
        elif code == 404:
            return None
        else:
            raise Exception('Error retrieving value, {0}/{1}, error, ${2}'.format(self.name, key, code))

    def insert(self, doc, key=None):
        if not key:
            key = ''

        resource = '{base}/{collection}/{key}'.format(base=COLLECTION_RESOURCE,
                                                      collection=self.name,
                                                      key=key)

        code, headers, data = self.connection.post(resource, json.dumps(doc), DEFAULT_HEADERS)

        if key and code == 204:
            return key
        elif not key and code == 201:
            location = headers['location']
            pos = location.rfind('/')
            if pos < 0:
                raise Exception("Error inserting value, could not determine key")

            key = location[pos:]

            return key
        else:
            raise Exception("Error inserting value, {0}/{1}, error {2}".format(self.name, key, code))

    #def update(self, doc):
    #    pass

    def delete(self, key):
        resource = '{base}/{collection}/{key}'.format(base=COLLECTION_RESOURCE,
                                                      collection=self.name,
                                                      key=key)

        code, headers, data = self.connection.delete(resource)

        if code == 204:
            return True
        elif code == 404:
            return False
        else:
            raise Exception("Error deleting value, {0}/{1}, error, {2}".format(self.name, key, code))

    def find_one(self, query):
        resource = '{base}/{collection}/query/one'.format(base=COLLECTION_RESOURCE, collection=self.name)

        #print resource, query

        code, headers, data = self.connection.put(resource, json.dumps(query), DEFAULT_HEADERS)

        if code == 200:
            return json.loads(data)
        elif code == 404:
            return {}
        else:
            raise Exception("Error querying for single result, {0}/query/one, error, {1}".format(self.name, code))

    def find(self, query):
        resource = '{base}/{collection}/query/all'.format(base=COLLECTION_RESOURCE, collection=self.name)

        code, headers, data = self.connection.put(resource, json.dumps(query), DEFAULT_HEADERS, debug=DEBUG)

        if code == 200:
            data_resp = json.loads(data)
            if data_resp == []:
                return None
            else:
                return Result(self, query, data_resp)
        elif code == 404:
            return {}
        else:
            raise Exception("Error querying for result, {0}/query, {1}, error {2}\n{3}".format(self.name,
                                                                                               json.dumps(query),
                                                                                               code,
                                                                                               data))

    def set_schema(self, schema_doc):
        resource = '{base}/{collection}/schema'.format(base=COLLECTION_RESOURCE,
                                                       collection=self.name)

        code, headers, data = self.connection.put(resource, json.dumps(schema_doc), DEFAULT_HEADERS)

        if code == 204:
            return True
        else:
            raise Exception("Error setting schema for {1}, {0}, error {2}\n{3}".format(self.name,
                                                                                       json.dumps(schema_doc),
                                                                                       code,
                                                                                       data))

    def get_schema(self):
        resource = '{base}/{collection}/schema'.format(base=COLLECTION_RESOURCE,
                                                       collection=self.name)

        code, headers, data = self.connection.get(resource, DEFAULT_HEADERS)

        if code == 200:
            return json.loads(data)
        elif code == 404:
            return [{}]
        else:
            raise Exception("Unexpected response retrieving schema for {0}, error {1}\n{2}".format(self.name,
                                                                                                   code,
                                                                                                   data))

    def delete_schema(self):
        resource = '{yz_base}index/{collection}{internal_suffix}'.format(yz_base=YZ_RESOURCE,
                                                                         collection=self.name,
                                                                         internal_suffix=INTERNAL_INDEX_SUFFIX)

        code, headers, data = self.connection.delete(resource, root='')

        if code == 204:
            return True
        else:
            raise Exception("Unexpected response deleting existing schema {0}, error {1}\n{2}".format(self.name,
                                                                                                      code,
                                                                                                      data))

    def count(self, query):
        pass

    def aggregate(self, query):
        pass