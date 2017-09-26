
import geopandas as gpd
import pandas as pd
import numpy as np
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main():
    '''
    Add service table to the database 
    '''

    # specify the filename
    shp_fn = '../data/seattle_contracts/seattle-contracts-geocoded.shp'
    block_fn = '../data/block_data/sea_blocks_wgs84.shp'
    db_fn = '../query_results/sea_5km.db'

    # specify the attributes to keep
    attributes = {
                'name' : 'contracts',
                'fields' : ['ContractNo', 'Project', 'LineofBiz', 'TotalBudgt'],
                'dtypes' : ['VARCHAR (40)', 'VARCHAR (40)', 'VARCHAR (40)', 'REAL'],
                'subset' : [['LineofBiz',['ADS Area Agency on Aging', 'ADS Self Sufficiency']]],
                'index' : 'ContractNo',
                }

    # take the subset
    sf = ImportData(shp_fn, attributes)

    # pair with dest_id
    GeometricIntersect(sf, block_fn)
    
    # create dataframe
    attributes['fields'].append('dest_id')
    attributes['dtypes'].append('VARCHAR (15)')
    fields = attributes['fields']
    sf[attributes['index']] = sf.index
    df = sf[fields]
    df = df[df.dest_id != 'None']
    df = df[fields]

    # add to table
    WriteDB(df, db_fn, attributes)


def ImportData(shp_fn, attributes):
    '''
    Read Shapefile and subset by attributes
    '''
    logger.info('Loading spatial data')

    # import shapefile
    sf = gpd.read_file(shp_fn)

    # get fields from attributes
    fields = attributes['fields'] + ['geometry']
    
    # create dataframe
    sf = sf[fields]    
    sf.set_index(attributes['index'], inplace=True)

    # do any required subset
    if 'subset' in attributes:
        for field in attributes['subset']:
            key = field[0]
            sf = sf[(sf[key].str.contains("ADS Area Agency on Aging")) | (sf[key].str.contains("ADS Self-Sufficiency"))]
    # clean numbers
    def CurrencyToFloat(x):
        return float(x[1:].replace(',',''))

    if 'TotalBudgt' in sf:
        sf['TotalBudgt'] = sf['TotalBudgt'].apply(CurrencyToFloat)

    return sf


def GeometricIntersect(sf, block_fn):
    '''
    Determine block id that the service is in
    '''

    # import blocks
    bf = gpd.read_file(block_fn)

    logger.info('Intersecting points and polygons')
    # loop through points and determine block id
    
    block_id = []
    for i in range(len(sf)):
        pt = sf['geometry'][i]
        intersect_bool = bf.intersects(pt)
        if intersect_bool.sum() > 0:
            block_id.append(bf['BLOCKID10'][bf.intersects(pt)].values[0])
        else:
            block_id.append('None')
            
    
    sf['dest_id'] = block_id 

    logger.info('Block information added')

    return sf


def WriteDB(df, db_fn, attributes):
    '''
    Add table to db
    '''
    
    logger.info('Writing to DB')
    atr = Bunch(attributes) 

    #Initialize connections to .db
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    #Create tables
    create_table_str = [ x + ' ' + y for (x, y) in zip(atr.fields, atr.dtypes)] 
    create_table_str = ', '.join(create_table_str)
    create_table = 'CREATE TABLE {}('.format(atr.name) + create_table_str + ')'
    cursor.execute(create_table)
    db.commit()

    #Populate tables with source data
    vals = ['?'] * len(atr.fields)
    orig_str = 'INSERT INTO {}('.format(atr.name) + ', '.join(atr.fields) + ') VALUES(' + ', '.join(vals) + ')'  

    for row in df.iterrows():
        include = tuple([x for x in row[1]])
        cursor.execute(orig_str, include)
        db.commit()

    logger.info('Complete')

class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)



if __name__ == '__main__':
    main()
