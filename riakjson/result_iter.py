__author__ = 'dankerrigan'

import json


def result_iter(query_fun, query, result, result_limit=None):
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