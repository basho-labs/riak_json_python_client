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