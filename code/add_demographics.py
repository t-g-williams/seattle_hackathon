import pandas as pd
import sqlite3
import numpy as np
import code
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    '''
    Append selected demographic data to the origin database
    '''

    # specify the file names
    dem_fn = '../data/nhgis0004_csv/nhgis0004_ds172_2010_block.csv'
    db_fn =  '../query_results/combined-data_5km.db'

    # specify the attributes
    attr = {
            'field' : 'pop',
            'census_code' : 'H7V001',
            'custom' : False
            }

    # connect to database
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # import and subset demographics
    df = ImportDemographics(dem_fn, attr, cursor)

    # Write to database
    WriteDB(df, db, attr)
    db.close()

def ImportDemographics(dem_fn, attr, cursor):
    ''' 
    dem_fn (str) = full path and filename of demographic data (stored as csv)
    dem_orig_id (str) = column name in the demographic data to be linked to the orig_id in the database
    dem_colname (list of str) = column name(s) of the demographic data to be added to the database
    db_fn (str) = full path and filename of the origins database
    '''
    logger.info('Importing demographics')

    # import demographic data (csv)
    df = pd.read_csv(dem_fn, header = 0,  
        dtype = {'block_id' : str, 'STATEA' : str, 'COUNTYA' : str, 'TRACTA' : str, 'BLOCKA' : str})
    # create the BLOCKID10 str. note: this is a 15 character string comprising 
        #STATEFP10, length: 2; COUNTYFP10, length: 3; TRACTCE10, length: 6; BLOCKCE, length: 4 )
    df['block_id'] = df['STATEA'] + df['COUNTYA'] + df['TRACTA'] + df['BLOCKA']
    df.set_index('block_id', inplace=True)

    # get the list orig_id values from the orig table
    cursor.execute("SELECT orig_id FROM orig")
    orig_ids = cursor.fetchall()
    orig_ids = [x[0] for x in orig_ids]

    # subset by rows to include only block groups in db
    df = df.ix[orig_ids]

    ###
    # If you need to do calculations on the dataset, do it here
    if attr['custom']:
        df = CalculateDemographic(df,attr)
    else:
        # subset by census code columns 
        df = df[attr['census_code']]
    
    return df


def CalculateDemographic(df, attr):
    '''
    If you need to calculate values for the demographic
    e.g. merge add groups
    do it here
    '''
    logger.info('Calculating other fields')

    # which census codes will you sum?
    census_codes = ['H76020', 'H76021', 'H76022', 'H76023', 'H76024', 'H76025',
                    'H76044', 'H76045', 'H76046', 'H76047', 'H76048', 'H76049']

    # var name defined in main
    var_name = attr['field']

    # add column
    df[var_name] = df[census_codes].sum(1)

    #subset
    df = df[var_name]
    return df


def WriteDB(df, db, attr):
    '''
    Add table to db
    '''

    logger.info('Writing to DB')

    #Initialize connections to .db
    cursor = db.cursor()
    # code.interact(local=locals())
    # add column
    col_name = attr['field']
    add_col_str = "ALTER TABLE orig ADD COLUMN {} INT".format(col_name)
    db.execute(add_col_str) 

    for i in range(len(df)):
        add_data_str = "UPDATE orig SET {} =(?) WHERE orig_id=(?)".format(col_name)
        value = int(df.values[i])
        idx = df.index[i]
        db.execute(add_data_str, (value, idx))
    # commit
    db.commit()

    logger.info('Complete')



if __name__ == "__main__":
    main()




# my_data = ({id=1, value='foo'}, {id=2, value='bar'})
# # string add
# data = df.T.to_dict('tuple')
# add_data_str = "UPDATE orig SET {} =:value WHERE orig_id=:id".format(col_name)
# cursor.executemany(add_data_str, data)
# db.commit()

    

#     # add
#     add_data_str = "UPDATE orig SET {} = ?".format(col_name)
#     # list of tuples
#     values = list(zip(df.tolist()))
#     # add all into table
#     db.executemany(add_data_str, values)
#     # commit
#     db.commit()



# def scratch():
#     # get table names
#     nms = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     [print(nm) for nm in nms]

#         # create new table in database containing relevant info
#     new_table_str = "CREATE TABLE dem(orig_id VARCHAR (20), " + ' REAL, '.join(dem_colnames) + ' REAL)'
#     cursor.execute(new_table_str)
#     # add data to this table
#     add_dem_data_str = "INSERT INTO dem VALUES"

#     # create new table with origin ids from the demographic data
#     new_table_str = "CREATE TABLE dem(orig_id VARCHAR (20))"
#     cursor.execute(new_table_str)
#     db.execute("INSERT INTO dem(orig_id) VALUES(?)", dem_data[][dem_orig_id])


#     # add demographic columns to this table
#     add_col_str = "ALTER TABLE dem ADD COLUMN {} REAL"
#     add_data_str = "INSERT INTO dem({}) VALUES(?)"
#     for c_name in dem_colnames:
#         # add column to table
#         db.execute(add_col_str.format(c_name))
#         # add data
#         db.execute(add_data_str.format(c_name), dem_data[][c_name])


#     vals = ['?'] * len(atr.fields)
#     orig_str = 'INSERT INTO {}('.format(atr.name) + ', '.join(atr.fields) + ') VALUES(' + ', '.join(vals) + ')'  

#     for row in df.iterrows():
#         include = tuple([x for x in row[1]])
#         cursor.execute(orig_str, include)
#         db.commit()

#     #Create tables
#     create_table_str = [ x + ' ' + y for (x, y) in zip(atr.fields, atr.dtypes)] 
#     create_table_str = ', '.join(create_table_str)
#     create_table = 'CREATE TABLE {}('.format(atr.name) + create_table_str + ')'
#     cursor.execute(create_table)
#     db.commit()

#     #Populate tables with source data
#     vals = ['?'] * len(atr.fields)
#     orig_str = 'INSERT INTO {}('.format(atr.name) + ', '.join(atr.fields) + ') VALUES(' + ', '.join(vals) + ')'  

#     for row in df.iterrows():
#         include = tuple([x for x in row[1]])
#         cursor.execute(orig_str, include)
#         db.commit()