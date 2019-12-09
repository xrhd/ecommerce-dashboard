# !/usr/bin/python

import psycopg2
from config import config


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        "DROP TABLE IF EXISTS products CASCADE;",
        "DROP TABLE IF EXISTS categories CASCADE;",
        "DROP TABLE IF EXISTS reviews CASCADE;",
        "DROP TABLE IF EXISTS similars CASCADE;",
        "DROP TABLE IF EXISTS products_categories CASCADE;",
        """
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            asin VARCHAR,
            title VARCHAR(500),
            product_group VARCHAR(20),
            salesrank INTEGER,
            reviews_downloaded INTEGER,
            similar_amount INTEGER,
            reviews_total INTEGER,
            reviews_avg_rating REAL 
        );
        """,
        """
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            hierarchy VARCHAR(255)
        );
        """,
        """
        CREATE TABLE reviews (
            product_id INTEGER REFERENCES products(id),
            time DATE,
            customer_id CHAR(20),
            rating INTEGER,
            votes INTEGER,
            helpful INTEGER,
            PRIMARY KEY (product_id, time, customer_id)
        );
        """,
        """
        CREATE TABLE similars (
            origin_product_id INTEGER REFERENCES products(id),
            destiny_product_id INTEGER REFERENCES products(id),
            PRIMARY KEY (origin_product_id, destiny_product_id)
        );
        """,
        """
        CREATE TABLE products_categories (
            product_id INTEGER REFERENCES products(id),
            category_id INTEGER REFERENCES categories(id),
            PRIMARY KEY (product_id, category_id)
        );
        """)
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()