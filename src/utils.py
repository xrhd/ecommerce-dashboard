from functools import reduce

products = [
    'id',
    'ASIN' ,
    'title' ,
    '"group"' ,
    'salesrank',
    'reviews_downloaded',
    'similar_amount',
    'reviews_total',
    'reviews_avg_rating'
]

categories = [
    'id',
    'hierarchy'
]

reviews = [
    'product_id',
    'time',
    'customer_id',
    'rating',
    'votes',
    'helpfulness',
]

schemas = {
    'products': products,
    'categories': categories,
    'reviews': reviews
}

def select_from_fulldump(dumps, t_name, n=None):
    t_name = t_name.lower() 
    cols = schemas[t_name]
    if not n:
        n = len(dumps)
    if t_name == 'products':
        return ({i:dump[i] for i in cols if i in dump} for dump in dumps[:n])
    else:
        def add_pid(dump, pid):
            for d in dump:
                d['product_id']=pid
            return dump
        items = reduce(lambda l1, l2: l1+l2 , [
            add_pid(dump[t_name], dump['id']) 
            for dump in dumps[:10] if t_name in dump
        ])
        return [{col:item[col] for col in cols if col in item} for item in items]