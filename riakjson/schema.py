__author__ = 'dankerrigan'

from collections import OrderedDict


class Schema(object):
    def __init__(self):
        self._schema = list()

    def _add_field(self, field_name, field_type):
        self._schema.append(OrderedDict([('name', field_name), ('type', field_type)]))

    def number(self, field_name):
        self._add_field(field_name, 'number')
        return self

    def integer(self, field_name):
        self._add_field(field_name, 'integer')

    def text(self, field_name):
        self._add_field(field_name, 'text')
        return self

    def string(self, field_name):
        self._add_field(field_name, 'string')
        return self

    def multi_string(self, field_name):
        self._add_field(field_name, 'multi_string')
        return self

    def geo(self, field_name):
        self._add_field(field_name, 'geo')
        return self

    def build(self):
        return self._schema