# general database functions

import sqlite3
import pandas as pd

def getTabNames(db):
    nms = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    names = [nm[0] for nm in nms]
    return(names)

def getTable(cursor, table_name, col_nums, col_names):
    '''
    get table 'table_name' from the database
    # convert to pandas data frame
    # col_nums = a list of column numbers
    # col_names = a list of column names
    '''
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tuple_data = tmp.fetchall()
    # convert to pandas dataframe
    data_list = [[row[i] for i in col_nums] for row in tuple_data]
    contract_pd = pd.DataFrame(data_list, columns=col_names)
    return(contract_pd)

def getColNames(cursor, table_name):
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tmp.fetchone()
    nmes = [description[0] for description in tmp.description]
    # print(nmes)
    return(nmes)


