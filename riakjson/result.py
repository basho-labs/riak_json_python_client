__author__ = 'dankerrigan'

from query import Query

class Result(object):
    def __init__(self, collection, query, response_doc):
        self.collection = collection
        self.query = query
        self.total = response_doc.get('total', 0)
        self.page = response_doc.get('page', 0)
        self.per_page = response_doc.get('per_page', 0)
        self.num_pages = response_doc.get('num_pages', 0)
        self.raw_data = response_doc.get('data', [])
        self.stats = Stats(response_doc.get('stats', dict()))
        self.facets = response_doc.get('facets', dict())

    def objects(self, result_limit=None):
        _query = Query(query=self.query)
        try:
            result_count = 0

            for page in xrange(1, self.num_pages + 1):
                if page != 1:
                    _query.offset(page)
                    result = self.collection.find(_query.build())
                    data = result.raw_data
                else:
                    data = self.raw_data

                for doc in data:
                    if result_limit and result_count >= result_limit:
                        break
                    yield doc
                    result_count += 1

                if result_limit and result_count >= result_limit:
                    break

        except Exception as e:
            print("result objects iterator encountered unexpected error, {0}".format(e))


class Stats(object):
    def __init__(self, stats_doc):
        self.fields = dict()
        for field, doc in stats_doc.get('stats_fields', dict()).items():
            self.fields[field] = StatsField(doc)


class StatsField(object):
    def __init__(self, stats_doc):
        self.min = stats_doc.get('min', None)
        self.max = stats_doc.get('max', None)
        self.count = stats_doc.get('count', None)
        self.missing = stats_doc.get('missing', None)
        self.sum = stats_doc.get('sum', None)
        self.sum_of_squares = stats_doc.get('sumOfSquares', None)
        self.mean = stats_doc.get('mean', None)
        self.facets = stats_doc.get('facets', {})