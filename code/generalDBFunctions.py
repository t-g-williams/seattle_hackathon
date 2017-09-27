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



def addPdToDb(d_frame, cursor, new_table_name, cols_str, col_names):
    '''
    add a pandas dataframe (d_frame) to a database (db)
    NOTE: this code is not generalizable (it adds the 3rd column as an int)
    create new table
    '''
    add_table_str = "CREATE TABLE {}({})".format(new_table_name, cols_str)
    cursor.execute(add_table_str)
    # add data
    add_data_str = "INSERT INTO {}({}) VALUES(?,?,?)".format(new_table_name, ', '.join(col_names))
    for i in range(d_frame.shape[0]):
        cursor.execute(add_data_str, (d_frame.ix[i,0],d_frame.ix[i,1],int(d_frame.ix[i,2])))

