__author__ = "dankerrigan"

from collections import OrderedDict

ASCENDING = 1
DESCENDING = -1


def and_args(*args):
    return {"$and": list(args)}


def or_args(*args):
    return {"$or": list(args)}


def eq(key, value):
    return {key: value}


def between(field, from_value, to_value):
    return and_args(gte(field, from_value), lte(field, to_value))


def gt(field, value):
    return {field: {"$gt": value}}


def gte(field, value):
    return {field: {"$gte": value}}


def lt(field, value):
    return {field: {"$lt": value}}


def lte(field, value):
    return {field: {"$lte": value}}


def ne(field, value):
    return {field: {"$ne": value}}


def nin(field, array_value):
    return {field: {"$nin": array_value}}


def regex(field, regex_value):
    return {field: {"$regex": regex_value}}


class Query(object):
    def __init__(self, query=dict()):
        self._order = None
        self._limit = None
        self._offset = None
        for key in ['$sort', '$per_page', '$page']:
            if key in query:
                if key == '$sort':
                    self.order(query[key])
                elif key == '$per_page':
                    self.limit(query[key])
                elif key == '$page':
                    self.offset(query[key])

                query.pop(key)

        self._query = query

    def build(self):
        query = OrderedDict(self._query)
        if self._order:
            query['$sort'] = self._order
        if self._limit:
            query['$per_page'] = self._limit
        if self._offset:
            query['$page'] = self._offset

        return query

    def select(self, *args):
        raise Exception("select not yet implemented")

    def where(self, query):
        for key, value in query.items():
            if key in self._query:
                if key == '$and' or key == '$or':
                    self._query[key].append(value)
                else:
                    self._query[key] = value
            else:
                self._query[key] = value

        return self

    def order(self, order_dict):
        self._order = order_dict

        return self

    def limit(self, value):
        self._limit = value

        return self

    def offset(self, value):
        self._offset = value

        return self