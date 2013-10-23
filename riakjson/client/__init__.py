__author__ = 'dankerrigan'

from connection import Connection
from ..collection import Collection


class Client(object):


    def __init__(self, conn=Connection()):
        self.conn = conn

        self.collections = dict()

    def __getitem__(self, name):
        return self.get_collection(name)

    def __getattr__(self, name):
        return self.get_collection(name)

    def get_collection(self, name):
        if name not in self.collections:
            collection = Collection(name, self.conn)
            self.collections[name] = collection

        return self.collections[name]