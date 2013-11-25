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