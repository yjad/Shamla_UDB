#! DB Handling
import  logging
logging.basicConfig(level=logging.DEBUG)

ERROR_OPENING_DB = -1
ERROR_READINGS_RECS = -2
ERROR_CLOSING_CONN = -3

import sqlite3
from sqlite3 import Error

DB_FILE_NAME = r"main.sqlite"
def exec_db_cmd(cmd):
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
    except Error as e:
        print(f'error opening database: {e}')
        return None, ERROR_OPENING_DB
    try:
        rs = conn.execute(cmd)
    except Error as e:
        print ('error reading record: ', e)
        conn.close()
        return None, ERROR_READINGS_RECS

    rec_list = rs.fetchall()

    try:
        conn.close()
    except Error as e:
        logging.debug (f'Error closing conn/cursor: {e}')
        return None, ERROR_CLOSING_CONN
    logging.debug(f"no of rows: {len(rec_list)}")
    print (rec_list)
    return rec_list


def query_db(query, one=False):
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
    except Error as e:
        print(f'error opening database: {e}')
        return None, ERROR_OPENING_DB

    cur = conn.cursor()
    cur.execute(query)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.connection.close()
    return (r[0] if r else None) if one else r


def categories_db(cmd):

    logging.debug(cmd)
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
    except Error as e:
        print(f'error opening database: {e}')
        return None, ERROR_OPENING_DB

    try:
        categs = conn.execute(cmd)
    except Error as e:
        print ('error reading record: ', e)
        conn.close()
        return None, ERROR_READINGS_RECS

    categ_list = categs.fetchall()

    try:
        conn.close()
    except Error as e:
        logging.debug (f'Error closing conn/cursor: {e}')
        return None, ERROR_CLOSING_CONN
    logging.debug(f"no of rows: {len(categ_list)}")
    return categ_list

def books_db(cmd):

    #return exec_db_cmd(cmd)
    return query_db(cmd)


def update_todo_db_record(rec):

    sql = f"update todo set title = '{rec[1]}', desc = '{rec[2]}', status = '{rec[3]}' where ID = {rec[0]}"
    print (sql)
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
    except Error as e:
        print(f'error opening database: {e}')
        conn.close()
        return None, -1

    cur = conn.cursor()
    try:
        tasks = cur.execute(sql)
    except Error as e:
        print ('error updating record: ', e)
        conn.close()
        return -6   # error updating rec

    if cur.rowcount == 0:
        conn.close()
        return -7   # record not found

    try:
        conn.commit()
        conn.close()
    except Error as e:
        print ('Error closing conn/cursor: ', e)
        return -5
    return 0

def delete_todo_db_record(task_id):

    sql = f"delete from todo where ID = {task_id}"
    print (sql)
    try:
        conn = sqlite3.connect(DB_FILE_NAME)
    except Error as e:
        print(f'error opening database: {e}')
        conn.close()
        return None, -1

    cur = conn.cursor()
    try:
        tasks = cur.execute(sql)
    except Error as e:
        print ('error deleting record: ', e)
        conn.close()
        return -8   # error updating rec

    no_deleted_records = cur.rowcount
    if cur.rowcount == 0:
        conn.close()
        return -7   # record not found

    try:
        conn.commit()
        conn.close()
    except Error as e:
        print ('Error closing conn/cursor: ', e)
        return -5
    return no_deleted_records

"""
rec = [8,'updated title', 'updated desc', 'Done']
status = delete_todo_db_record(8)
print (status)
"""