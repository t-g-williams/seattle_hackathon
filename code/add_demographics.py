import pandas as pd
import sqlite3
import numpy as np
import code
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# specify the file names
dem_fn = '../data/demographic/nhgis0002_ds172_2010_block.csv'
db_fn =  '../query_results/sea_5km.db'

fields = [('pop_female','H76026'),('pop_below_10',True),('pop_color',True), ('pop_total','H76001'), ('pop_over_65', True)]

def main(field, dem_fn, db_fn):

    ''' 
    Append selected demographic data to the origin database
    '''

    # for x in range(0,len(fields)):

    # specify the attributes
    attr = {
            'field' : field[0], #'pop_above_65', 'pop_over_65', 'female','pop_below_10','people_of_color'
            'census_code' : field[1],
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
    logger.info('Importing demographics: ' + attr['field'])

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
    if isinstance(attr['census_code'], str):
        df = df[attr['census_code']]
    else:
        # subset by census code columns 
        df = CalculateDemographic(df,attr['field'])
    
    return df


def CalculateDemographic(df, var_name):
    '''
    If you need to calculate values for the demographic
    e.g. merge add groups
    do it here
    '''
    logger.info('Calculating other fields')

    # which census codes will you sum?
    if var_name == 'pop_over_65':
        # consider population over 65
        census_codes = ['H76020', 'H76021', 'H76022', 'H76023', 'H76024', 'H76025',
                        'H76044', 'H76045', 'H76046', 'H76047', 'H76048', 'H76049']
        
        # add column
        df[var_name] = df[census_codes].sum(1)
    elif var_name == 'pop_color':
        # consider people of color
        # census_code = [,'XXXX']
        # add column people of color = (Total - white)
        df[var_name] = df['H7X001'] - df['H7X002']  ###### add the code for number of white people
    # consider population below 10 
    elif var_name == 'pop_below_10':
        # conside people of color 
        census_codes = ['H76003','H76004',
                        'H76027','H76028'] 
        df[var_name] = df[census_codes].sum(1)
    # consider female population 
    elif var_name == 'pop_female':
        census_codes = ['H76027', 'H76028', 'H76029','H76030','H76031','H76032','H76033','H76034',
                        'H76035','H76036','H76037','H76038','H76039','H76040','H76041','H76042',
                        'H76043','H76044','H76045','H76046','H76047','H76048','H76049']
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
    for field in fields:
        main(field, dem_fn, db_fn)


