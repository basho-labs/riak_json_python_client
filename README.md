# Riak JSON Python Client

## Requirements

+ Python 2.7
+ [requests](http://www.python-requests.org/en/latest/user/install/)
+ Riak with [riak_json](https://github.com/basho-labs/riak_json), installed, enabled in ```riak.conf```, and running

## Installation

    git clone http://github.com/basho/riak_json_python_client
    cd riak_json_python_client
    python setup.py install

## Usage

### Unit Testing
Note: The unit tests assume that Riak is listening on the default HTTP port of 8098.
```
python -m unittest riakjson.tests.query_tests
```

### Create a connection

    from riakjson.client import Client

    client = Client(Connection(host='127.0.0.1', port=8000))

### Create or Use an existing collection

     customers = client.customers

     ## or you can also use dictionary syntax

     purchases = client['purchases']

### Insert a record

    ## Customer
    customer = {'name': 'Json Argo',
                'Address': '123 Fake Street',
                'City': 'Ancient',
                'State': 'Greece',
                'Age': 23}

    customer_key = customers.insert(customer)

    ## '/TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'

    ## Insert record with predefined key
    customer2 = {'name': 'Dan Gerr',
                 'Address': '1000 Hoover Drive',
                 'City': 'Ferrytown',
                 'State': 'WV',
                 'Age': 33}
    customer2_key = 'cust_key_12345'
    customers.insert(customer2, customer2_key)
    ## 'cust_key_12345'

    purchase = {'id': 'Golden_Fleece_5',
                'customer_id': customer_key}

    purchase_key = purchases.insert(purchase)

    ## '/XLtp2f3m5yAwHGWRjvlbPs9yO9x'

### Retrieve a record

    customer_record = customers.get(customer_key)

    ## u'{"Age":23,"City":"Ancient","State":"Greece","name":"Json Argo","Address":"123 Fake Street"}'

    purchase_record = purchases.get(purchase_key)

    ## u'{"customer_id":"/TgFKK4uUZKSpyXkMGGQUMhcZ8TQ","id":"Golden_Fleece_5"}'

### Search for a record

#### Exact field match, with max of single returned record

    query = {'name': 'Json Argo'}

    customers.find_one(query)

    ## {u'City': u'Ancient', u'name': u'Json Argo', u'Age': 23, u'State': u'Greece', u'Address': u'123 Fake Street', u'_id': u'TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'}

#### Find all users whose name starts with Json

    ## find all users whose name begins with Json, field names are case sensitive
    query = {'name': 'Json*'}

    result = customers.find(query)

    ## <riakjson.result.Result object at 0x10e884910>

    # iterate results
    for item in result.objects():
        print item

        ## {u'City': u'Ancient', u'name': u'Json Argo', u'Age': 23, u'State': u'Greece', u'Address': u'123 Fake Street', u'_id': u'TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'}

#### Range Search

    ## Age >= 21 and Age <= 25
    query = {'$and': [{'Age': {'$gte': 21}}, {'Age': {'$lte': 25}}]}

    # create a list from the generator
    list(customers.find(query).objects())
    [{u'City': u'Ancient', u'name': u'Json Argo', u'Age': 23, u'State': u'Greece', u'Address': u'123 Fake Street', u'_id': u'TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'}]

#### Use Query object and helper methods to construct queries

    from riakjson.query import Query
    from riakjson.query import ASCENDING, regex, gt

    ## State = W.* AND Age greater than 30
    query = Query(and_args(regex('State', 'W.*'), gt('Age', 30)))
                .limit(1)
                .order(ASCENDING)

    result = customers.find(query.build())

### See riakjson/examples/demo.py for more usage examples





