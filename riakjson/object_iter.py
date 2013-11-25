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


def object_iter(query_fun, query, result, result_limit=None):
    try:
        num_pages = result['num_pages']

        data = result['data']

        result_count = 0

        for page in xrange(1, num_pages + 1):
            if page != 1:
                query.offset(page)
                result = query_fun(query.build(), raw_result=True)
                data = result['data']

            for doc in data:
                if result_limit and result_count >= result_limit:
                    break
                yield doc
                result_count += 1

            if result_limit and result_count >= result_limit:
                break

    except Exception as e:
        print("result_iter encountered unexpected error, {0}".format(e))