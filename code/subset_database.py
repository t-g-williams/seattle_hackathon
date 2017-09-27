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
import generalDBFunctions as db_fns
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# file names and parameters
db_fn =  '../query_results/sea_5km.db'
max_dur = 30*60 # 30 minutes

def main(db_fn, max_dur):
    # create connection to the database
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # create a dataframe with only the relevant O-D pairs
    if 'destsubset' not in db_fns.getTabNames(db):
        createSubsetDataframe(cursor, max_dur)
        db.commit()
    else:
        logger.info('destsubset table already exists')

    db.close()

def createSubsetDataframe(cursor, max_dur):
    '''
    create a pandas dataframe containing the O-D pairs for destinations that contain services
    and add it to the database
    '''

    # # filter the database to O-D pairs with duration < specified time AND only dests that contain services
    logger.info('subsetting the walking results table')
    query_str = '''SELECT * FROM walking 
    WHERE duration < {}
    AND dest_id IN (SELECT dest_id FROM contracts)'''.format(max_dur)
    tmp = cursor.execute(query_str)
    od_pairs = tmp.fetchall()
    
    # create pandas dataframe
    data_list = [[row[0], row[1], row[2]] for row in od_pairs]
    col_names = ['orig_id', 'dest_id', 'walking_time']
    od_pairs = pd.DataFrame(data_list, columns=col_names)
    # write this as a table in the database...
    cols_str = "orig_id VARCHAR (15), dest_id VARCHAR (15), walking_time INT"
    
    # add to data base
    addPdToDb(od_pairs, cursor, 'destsubset', cols_str, col_names)
    return


def addPdToDb(d_frame, cursor, new_table_name, cols_str, col_names):
    '''
    add a pandas dataframe (d_frame) to a database (db)
    NOTE: this code is not generalizable (it adds the 3rd column as an int)
    create new table
    '''
    add_table_str = "CREATE TABLE {}({})".format(new_table_name, cols_str)
    cursor.execute(add_table_str)
    # add data
    val_str = ', '.join(len(col_names) * '?')
    add_data_str = "INSERT INTO {}({}) VALUES({})".format(new_table_name, ', '.join(col_names), val_str)
    for i in range(d_frame.shape[0]):
        cursor.execute(add_data_str, (d_frame.ix[i,0],d_frame.ix[i,1],int(d_frame.ix[i,2])))

if __name__ == '__main__':
    main(db_fn, max_dur)