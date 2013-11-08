__author__ = 'dankerrigan'

import requests


class Connection(object):
    def __init__(self, protocol='http', host='127.0.0.1', port=8098, root='document/'):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.root = root

    def _format_uri(self, resource, root=None):
        return '{protocol}://{host}:{port}/{root}{resource}'.format(protocol=self.protocol,
                                                              host=self.host,
                                                              port=self.port,
                                                                # potentially blank, test for None
                                                              root=self.root if root == None else root,
                                                              resource=resource)

    def get(self, resource, headers, debug=False, root=None):
        uri = self._format_uri(resource, root=root)

        resp = requests.get(uri, headers=headers, timeout=30)

        if debug:
            print 'Resource:', resource
            print 'Code:', resp.status_code
            print 'Response:', resp.text

        return resp.status_code, resp.headers, resp.text

    def post(self, resource, data, headers, debug=False, root=None):
        uri = self._format_uri(resource, root=root)

        resp = requests.post(uri, data=data, headers=headers, timeout=30)

        if debug:
            print 'Resource:', resource
            print 'Data:', data
            print 'Code:', resp.status_code
            print 'Response:', resp.text

        return resp.status_code, resp.headers, resp.text

    def put(self, resource, data, headers, debug=False, root=None):
        uri = self._format_uri(resource, root=root)

        resp = requests.put(uri, data=data, headers=headers, timeout=30)

        if debug:
            print 'Resource:', resource
            print 'Data:', data
            print 'Code:', resp.status_code
            print 'Response:', resp.text

        return resp.status_code, resp.headers, resp.text

    def delete(self, resource, root=None):
        uri = self._format_uri(resource, root=root)

        resp = requests.delete(uri)

        return resp.status_code, resp.headers, resp.text
