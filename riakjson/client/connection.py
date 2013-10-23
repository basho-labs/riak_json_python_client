__author__ = 'dankerrigan'

import requests


class Connection(object):
    def __init__(self, protocol='http', host='127.0.0.1', port=8000):
        self.protocol = protocol
        self.host = host
        self.port = port

    def _format_uri(self, resource):
        return '{protocol}://{host}:{port}/{resource}'.format(protocol=self.protocol,
                                                              host=self.host,
                                                              port=self.port,
                                                              resource=resource)

    def get(self, resource, headers):
        uri = self._format_uri(resource)

        resp = requests.get(uri, headers=headers)

        return resp.status_code, resp.headers, resp.text

    def post(self, resource, data, headers):
        uri = self._format_uri(resource)

        resp = requests.post(uri, data=data, headers=headers)

        return resp.status_code, resp.headers, resp.text

    def put(self, resource, data, headers):
        uri = self._format_uri(resource)

        resp = requests.put(uri, data=data, headers=headers)

        #print 'Resource:', resource
        #print 'Data:', data
        #print 'Code:', resp.status_code
        #print 'Response:', resp.text

        return resp.status_code, resp.headers, resp.text

    def delete(self, resource):
        uri = self._format_uri(resource)

        resp = requests.delete(uri)

        return resp.status_code, resp.headers, resp.text
