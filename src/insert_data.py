#!/usr/bin/python
 
from tqdm import tqdm
import psycopg2
from src.config import config
from psycopg2.extensions import AsIs

def insert_from_dict(t_name, p_dict):
    sql = f'insert into {t_name} (%s) values %s'
    columns = p_dict.keys()
    values = [p_dict[column] for column in columns]
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (AsIs(','.join(columns)), tuple(values)))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_from_dict_gen(t_name, p_dicts):
    sql = f'insert into {t_name} (%s) values %s'
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()

        for p_dict in tqdm(p_dicts):
            columns = p_dict.keys()
            values = [p_dict[column] for column in columns]
            try:
                cur.execute(sql, (AsIs(','.join(columns)), tuple(values)))
            except:
                continue

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()