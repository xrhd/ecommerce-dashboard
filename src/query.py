#!/usr/bin/python
 
import pandas as pd
import psycopg2
from src.config import config


def iter_row(cursor, size=10):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row

def query(sql, n=1):
    """ query part and vendor data from multiple tables"""
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        it = list(iter_row(cur, 10))
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        return it


import pandas as pd
def q_a(id=0):
    a1 = f'SELECT * FROM reviews WHERE product_id = {id} ORDER BY helpful DESC, rating DESC LIMIT 5;'
    a2 = f'SELECT * FROM reviews WHERE product_id = {id} ORDER BY helpful DESC, rating ASC LIMIT 5;'
    return pd.DataFrame(query(a1)+query(a2), columns=[
        'product_id',
        'time',
        'customer_id',
        'rating',
        'votes',
        'helpful',
    ]).sort_values('helpful', ascending=False)

def q_b(id=0):
    b = f'SELECT * FROM products p, products s, similiars WHERE p.id = {id} and similars.origin_product_id = {id} and s.id = similars.destiny_product_id and p.salesrank < s.salesrank;'
    return pd.DataFrame(query(b))


def q_c(id=0):
    c = f'SELECT DISTINCT ON (time) time, AVG (rating) OVER (ORDER BY time) AS avg_rating FROM reviews WHERE product_id = {id} ORDER BY time DESC;'
    c = pd.DataFrame(query(c), columns=['time', 'avg_rating']).sort_values('avg_rating', ascending=False)
    c.avg_rating = pd.to_numeric(c.avg_rating)
    return c

def q_d(n=10):
#     d = f'SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY salesrank DESC) AS r, products.* FROM products) x WHERE x.r <= 10;'
    d = 'SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY product_group ORDER BY salesrank ASC) AS r, products.* FROM products) x WHERE x.r <= 10 and x.salesrank >= 0;'
    d = pd.DataFrame(query(d), columns=[
        'i',
        'id',
        'ASIN' ,
        'title' ,
        "product_group" ,
        'salesrank',
        'reviews_downloaded',
        'similar_amount',
        'reviews_total',
        'reviews_avg_rating'
    ]).sort_values('salesrank')
    return d

def q_e():
    e = 'SELECT * FROM products WHERE reviews_avg_rating > 0 ORDER BY reviews_avg_rating DESC LIMIT 10;'
    return pd.DataFrame(query(e), columns=[
        'id',
        'ASIN' ,
        'title' ,
        "product_group" ,
        'salesrank',
        'reviews_downloaded',
        'similar_amount',
        'reviews_total',
        'reviews_avg_rating'
    ])

def q_f():
    f = 'SELECT categories.id, categories.hierarchy FROM products, categories, products_categories WHERE products.id = products_categories.product_id and categories.id = products_categories.category_id;'
    return pd.DataFrame(query(f))

def q_g():
    g = 'SELECT x.customer_id, x.product_group, COUNT(*) OVER (PARTITION BY x.product_group) AS quantity FROM (SELECT customer_id, product_group FROM products, reviews WHERE products.id = reviews.product_id) x GROUP BY (x.customer_id, x.product_group)'
    return pd.DataFrame(query(g),  columns=[
        'customer_id',
        'product_group',
        'count'
    ])

# if __name__ == "__main__":
#     print(q_a(8))