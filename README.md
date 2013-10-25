# Riak JSON Python Client

## Requiremnts

+ Python 2.7
+ requests

## Installation

    git clone http://github.com/basho/riak_json_python_client
    cd riak_json_python_client
    python setup.py install

## Usage

### Create a connection

    from riakjson.client import Client

    client = Client(host='127.0.0.1', port=8000)

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

    ## <generator object result_iter at 0x10530acd0>

    # iterate results
    for item in result:
        print item

        ## {u'City': u'Ancient', u'name': u'Json Argo', u'Age': 23, u'State': u'Greece', u'Address': u'123 Fake Street', u'_id': u'TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'}

#### Range Search

    ## Age >= 21 and Age <= 25
    query = {'$and': [{'Age': {'$gte': 21}}, {'Age': {'$lte': 25}}]}

    # create a list from the generator
    list(customers.find(query))
    [{u'City': u'Ancient', u'name': u'Json Argo', u'Age': 23, u'State': u'Greece', u'Address': u'123 Fake Street', u'_id': u'TgFKK4uUZKSpyXkMGGQUMhcZ8TQ'}]

