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
    return {field: {"$regex": "/" + regex_value + "/"}}

class Query(object):
    def __init__(self, query=dict()):
        self._order = None
        self._limit = None
        self._offset = None
        self._group_specs = list()
        self._stats = list()
        self._stats_args = dict()
        self._facets = list()
        self._facets_args = dict()
        for key in ['$sort', '$per_page', '$page', '$stats', '$facets']:
            if key in query:
                if key == '$sort':
                    self.order(query[key])
                elif key == '$per_page':
                    self.limit(query[key])
                elif key == '$page':
                    self.offset(query[key])
                elif key == '$stats':
                    self._stats.extend(query[key])
                elif key == '$facets':
                    self._facets.extend(query[key])

                query.pop(key)

        self._query = query

    def build(self):
        query = OrderedDict(self._query)
        if self._order:
            query['$sort'] = self._order
        if self._limit != None: # _limit can be 0 which evaluates to false
            query['$per_page'] = self._limit
        if self._offset:
            query['$page'] = self._offset

        if len(self._group_specs) > 0:
            query['$group'] = [group_spec.build() for group_spec in self._group_specs]
        if len(self._stats) > 0:
            query['$stats'] = self._stats
        if len(self._stats_args) > 0:
            for key, value in self._stats_args.items():
                query['$stats.' + key] = value
        if len(self._facets) > 0:
            query['$facets'] = self._facets
        if len(self._facets_args) > 0:
            for key, value in self._facets_args.items():
                query['$facets.' + key] = value

        return query

    def select(self, *args):
        raise Exception("select not yet implemented")

    def where(self, query):
        for key, value in query.items():
            if key in self._query:
                if key == '$and' or key == '$or':
                    self._query[key].extend(value)
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

    def add_grouping(self, group_spec):
        self._group_specs.append(group_spec)

    def stats_on(self, field):
        self._stats.append(field)

    def stats_args(self, args_dict):
        self._stats_args = args_dict

    def facet_on(self, field):
        self._facets.append(field)

    def facet_args(self, args_dict):
        self._facets_args = args_dict

class GroupSpec(object):
    def __init__(self, field=None, queries=list(), ranges=None, limit=0, start=1, sort=None):
        self.field = field
        self.queries = queries
        self.limit = limit
        self.start = start
        self.sort = sort

    def add_group_query(self, query):
        self.queries.append(query)

    def build(self):
        result = OrderedDict()

        if self.field:
            result['field'] = self.field
        if len(self.queries) > 0:
            result['queries'] = self.queries
        if self.limit:
            result['limit'] = self.limit
        if self.start:
            result['start'] = self.start
        if self.sort:
            result['sort'] = self.sort

        return result

class RangeSpec(object):
    def __init__(self, field, start=None, end=None, increment=None):
        self.field = field
        self.start = start
        self.end = end
        self.increment = increment

    def build(self):
        result = OrderedDict()
        result['field'] = self.field
        if self.start:
            result['start'] = self.start
        if self.end:
            result['end'] = self.end
        if self.increment:
            result['increment'] = self.increment
        return result

