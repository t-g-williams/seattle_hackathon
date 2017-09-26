# Inputs:     Database containing:
#                 - origin-destination pairs (table)
#                 - a subset of the destinations that contain services of interest (table)
#             Maximum duration of walking
# Output:     Table containing O-D pairs only for the destinations of interest

import pandas as pd
import numpy as np
import sqlite3
import code
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # file names and parameters
    # db_fn =  '../query_results/combined-data_5km_with_hssa.db'
    db_fn =  '../query_results/sea_5km.db'
    max_dur = 30*60 # 30 minutes

    # create connection to the database
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # create a dataframe with only the relevant O-D pairs
    if 'destsubset' not in getTabNames(db):
        createSubsetDataframe(cursor, max_dur)
        db.commit()

    db.close()

def createSubsetDataframe(cursor, max_dur):
    # create a pandas dataframe containing the O-D pairs for destinations that contain services
    # and adds it to the database

    # # get list of dest id's that contain the services of interest
    # tmp = cursor.execute("SELECT dest_id FROM contracts")
    # service_dest_ids_tuple = tmp.fetchall()
    # service_dest_ids = [x[0] for x in service_dest_ids_tuple]

    # # filter the database to O-D pairs with duration < specified time
    # tmp = cursor.execute("SELECT * FROM origxdest WHERE walking_time < {}".format(max_dur))
    logger.info('subsetting the walking results table')
    tmp = cursor.execute('''SELECT * FROM walking 
    WHERE duration < {}
    AND dest_id IN (SELECT dest_id FROM contracts)'''.format(max_dur))
    #
    od_pairs = tmp.fetchall()
    #
    # create pandas dataframe
    data_list = [[row[0], row[1], row[2]] for row in od_pairs]
    od_pairs = pd.DataFrame(data_list, columns=['orig_id', 'dest_id', 'walking_time'])
    # write this as a table in the database...
    # strings
    cols_str = "orig_id VARCHAR (15), dest_id VARCHAR (15), walking_time INT"
    col_names = ['orig_id', 'dest_id', 'walking_time']
    # convert index back to column and format data frame
    # od_pairs_subset['dest_id'] = od_pairs_subset.index
    # od_pairs_subset = od_pairs_subset[['orig_id', 'dest_id', 'walking_time']]
    # add to data base
    addPdToDb(od_pairs, cursor, 'destsubset', cols_str, col_names)
    return

def addPdToDb(d_frame, cursor, new_table_name, cols_str, col_names):
    # add a pandas dataframe (d_frame) to a database (db)
    # NOTE: this code is not generalizable (it adds the 3rd column as an int)
    # create new table
    add_table_str = "CREATE TABLE {}({})".format(new_table_name, cols_str)
    cursor.execute(add_table_str)
    # add data
    add_data_str = "INSERT INTO {}({}) VALUES(?,?,?)".format(new_table_name, ', '.join(col_names))
    for i in range(d_frame.shape[0]):
        # cursor.execute(add_data_str, (d_frame.ix[i,:]))
        cursor.execute(add_data_str, (d_frame.ix[i,0],d_frame.ix[i,1],int(d_frame.ix[i,2])))

def getTable(cursor, table_name, col_nums, col_names):
    # get table 'table_name' from the database
    # convert to pandas data frame
    # col_nums = a list of column numbers
    # col_names = a list of column names
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tuple_data = tmp.fetchall()
    # convert to pandas dataframe
    data_list = [[row[i] for i in col_nums] for row in tuple_data]
    contract_pd = pd.DataFrame(data_list, columns=col_names)
    return(contract_pd)

def getTabNames(db):
    nms = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    names = [nm[0] for nm in nms]
    return(names)

def getColNames(cursor, table_name):
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tmp.fetchone()
    nmes = [description[0] for description in tmp.description]
    # print(nmes)
    return(nmes)

if __name__ == '__main__':
    main()