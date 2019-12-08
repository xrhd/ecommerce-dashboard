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

# if __name__ == "__main__":
#     print(q_a(8))