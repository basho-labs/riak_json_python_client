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
        self._categorize_specs = list()
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

        if self._limit != None: # _limit can be 0 which evaluates to false
            query['$per_page'] = self._limit

        if self._offset:
            query['$page'] = self._offset

        if len(self._group_specs) > 0:
            query['$group'] = [group_spec.build() for group_spec in self._group_specs]

        if len(self._categorize_specs) > 0:
            query['$categorize'] = [cat_spec.build() for cat_spec in self._categorize_specs]

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

    def add_categorization(self, categorize_spec):
        self._categorize_specs.append(categorize_spec)

class GroupSpec(object):
    def __init__(self, field=None, queries=list(), limit=0, start=0, sort=None):
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
        if self.start != None:  # Range can be 0, which evaluates to false
            result['start'] = self.start
        if self.end:
            result['end'] = self.end
        if self.increment:
            result['increment'] = self.increment
        return result


class CategorizeSpec(object):
    def __init__(self, field=None, range_spec=None, queries=list(), limit=None, sort=None, start=None, stats=None):
        self.field = field
        self.range_spec = range_spec
        self.queries = queries
        self.limit = limit
        self.sort = sort
        self.start = start
        self.stats = stats

    def add_categorize_query(self, query):
        self.queries.append(query)

    def build(self):
        query = OrderedDict()
        if self.field:
            query['field'] = self.field
        if self.range_spec:
            query['range'] = self.range_spec.build()
        if len(self.queries) > 0:
            query['queries'] = self.queries
        if self.limit:
            query['limit'] = self.limit
        if self.sort:
            query['sort'] = self.sort
        if self.stats:
            query['stats'] = self.stats.build()
        return query

class StatsSpec(object):
    def __init__(self, category_field, stat_field):
        self.category = category_field
        self.stat = stat_field

    def build(self):
        query = OrderedDict()

        query['field'] = self.category
        query['calculate'] = self.stat

        return query