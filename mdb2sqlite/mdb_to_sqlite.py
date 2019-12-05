#! mdb_to_sqlite.py
import sys, subprocess, os
import sqlite3
from sqlite3 import Error
import time
import patoolib

MDB_TOOLS_DIR = r"D:\Yahia\Yahia\Python\Shamla_UDB\mdb2sqlite\mdbtools-win\\"

def mdb_2_sqlite(inp_file_name):
    if not os.path.exists(inp_file_name):
        print(f'file does not exist:{inp_file_name}')
        return
    t = time.time()

    file_path, ext = os.path.splitext(inp_file_name)
    if ext == '.rar':
        # extract to same direcory
        if os.path.exists(file_path + ".bok"):
            os.remove(file_path + ".bok")
        patoolib.extract_archive(inp_file_name, outdir= os.path.split(inp_file_name)[0])
        inp_file_name = file_path + ".bok"

    sql_path = file_path + ".sqlite"

    if os.path.exists(sql_path):
        os.remove(sql_path)

    try:
        conn = sqlite3.connect(sql_path)
    except Error as e:
        print(f"Error creating DB: {e}")
        return

    # Dump the schema for the DB
    schema = subprocess.Popen([MDB_TOOLS_DIR +"mdb-schema", "--drop-table", "--no-indexes",
                               "--no-relations",
                               inp_file_name, "mysql"],
                              stdout=subprocess.PIPE).communicate()[0]

    cur = conn.cursor()
    sql = 'BEGIN TRANSACTION;\n' + schema.decode('utf8') + "\nCommit"
    cur.executescript(sql)
    # Get the list of table names with "mdb-tables"
    table_names = subprocess.Popen([MDB_TOOLS_DIR +"mdb-tables", "-1", inp_file_name],
                                   stdout=subprocess.PIPE).communicate()[0]
    tables = table_names.splitlines()

    for table in tables:
        if table != '':
            #print (table)
            sql1 = subprocess.Popen([MDB_TOOLS_DIR +"mdb-export", "-I", "mysql", inp_file_name,
                                     table.decode()],
                                    stdout=subprocess.PIPE).communicate()[0]
            sql = sql1.decode('CP1256').encode('utf8')
            sql = 'BEGIN TRANSACTION;\n' + sql.decode('utf8') + "\nCommit"
            cur.executescript(sql)
    try:
        conn.close()
    except Error as e:
        print ('Error closing conn/cursor: ', e)
    print (f"file: {inp_file_name}: Time Taken: %.3f sec" % (time.time() - t))



#mdb_2_sqlite(r"D:\Yahia\Yahia\Python\Shamla_UDB\books\4739.bok")
#mdb_2_sqlite(r"D:\Yahia\Yahia\Python\Shamla_UDB\books\4512.mdb")
mdb_2_sqlite(r"D:\Yahia\Yahia\Python\Shamla_UDB\books\4739.rar")