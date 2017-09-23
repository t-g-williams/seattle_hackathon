'''
Database management for proximity querying
'''
import geopandas as gpd
import pandas as pd

import os
import sqlite3
import logging
logger = logging.getLogger(__name__)

def Init(db_fn, orig_fn, dest_fn, db_temp_fn):
    '''
    Generate a Sqlite3 temp data file and a combined-data.db.
    Transfer data from shapefile to temp data holding.
    '''

    #Load shapefiles
    logger.info('Writing new combined-data.db')
    orig_df = ReadShapefile('orig', orig_fn)
    dest_df = ReadShapefile('dest', dest_fn)

    #Clean up from before
    if os.path.isfile(db_temp_fn):
        os.remove(db_temp_fn)
        logger.info('Deleting old temp-data.db')

    #Initialize connections to .db's
    db = sqlite3.connect(db_temp_fn)
    cursor = db.cursor()
    db1 = sqlite3.connect(db_fn)
    cursor1 = db1.cursor()
    
    #Create tables
    create_table = 'CREATE TABLE {}({}_id VARCHAR (20), {}_lon REAL, {}_lat REAL)'
    for source in ['orig', 'dest']:    
        cursor.execute(create_table.format(source, source, source, source))
        db.commit()

    orig_tab = '''CREATE TABLE orig(orig_id VARCHAR (20), orig_lon REAL, orig_lat REAL)
    '''

    dest_tab = '''CREATE TABLE dest(dest_id VARCHAR (20), dest_lon REAL,
    dest_lat REAL)''' 
    cursor1.execute(orig_tab)
    cursor1.execute(dest_tab)
    db1.commit()

    #Populate tables with source data
    orig_str_a = 'INSERT INTO orig(orig_id, orig_lon, orig_lat) VALUES(?, ?, ?)'
    orig_str_b = '''INSERT INTO orig(orig_id, orig_lon, orig_lat) 
    VALUES(?, ?, ?)'''

    for orig_id, orig_dat in orig_df.iterrows():
        cursor.execute(orig_str_a, (orig_id, orig_dat['orig_lon'], orig_dat['orig_lat']))
        db.commit()
        include = (orig_id, orig_dat['orig_lon'], orig_dat['orig_lat'])
        cursor1.execute(orig_str_b, include)
        db1.commit()

    dest_str = 'INSERT INTO dest(dest_id, dest_lon, dest_lat) VALUES(?, ?, ?)'

    for dest_id, dest_dat in dest_df.iterrows():
        cursor.execute(dest_str, (dest_id, dest_dat['dest_lon'], dest_dat['dest_lat']))
        db.commit()
        cursor1.execute(dest_str, (dest_id, dest_dat['dest_lon'], dest_dat['dest_lat']))
        db1.commit()

    # init the dest-orig table
    cursor1.execute('''
        CREATE TABLE origxdest(orig_id VARCHAR (15), dest_id VARCHAR (15), euclidean INTEGER)
        ''')
    cursor1.execute('CREATE TABLE walking(orig_id VARCHAR (15), dest_id VARCHAR (15), duration INTEGER)')
   
    
    #Close connections to .db's
    db.close()
    db1.close()
    logger.info('Finished generating temp-data.db and combined-data.db')


def Write(mode, temp_fn, db_fn):
    #read to .db and clean up
    logger.info('Transfering data to .db')
    db_start = time.time()

    db = sqlite3.connect(db_fn) 
    cursor = db.cursor()

    with open(temp_fn, 'r') as fin: 
        dr = csv.DictReader(fin) 
        to_db = [(i['0'], i['1'], i['2']) for i in dr]
    insert_str = '''INSERT INTO {}(orig_id, dest_id, duration) VALUES (?, ?, ?)'''.format(mode)
    cursor.executemany(insert_str, to_db)
    db.commit()

    #Close connection to .db
    db.close()
    db_end = time.time()
    logger.info('Transferred data to .db ({} seconds)'.format(db_end - db_start))



def ReadShapefile(source, filename, sample = False):
    '''
    Read a shapefile containing the data for either the sources or destinations
    of a transit time matrix. Accordingly, the proper argument for source is
    'orig' or 'dest'.
    '''
    
    if source == 'orig':
        old_indx = 'BLOCKID10'
        new_indx = 'orig_id'
        new_lon = 'orig_lon'
        new_lat = 'orig_lat'
    elif source == 'dest':
        old_indx = 'BLOCKID10'
        new_indx = 'dest_id'
        new_lon = 'dest_lon'
        new_lat = 'dest_lat'
    else:
        return None

    # Load and parse the shapefile
    points = gpd.read_file(filename)

    # get the coordinates
    def getXY(pt):
        return (pt.x, pt.y)
    centroidseries = points['geometry'].centroid
    x,y = [list(t) for t in zip(*map(getXY, centroidseries))]
    # parse into dataframe
    if not(old_indx in points):
        logger.error(old_indx + ' is not a field in the shp')
        return None
    data = {new_indx : points[old_indx], new_lon : x, new_lat : y}
    df = pd.DataFrame(data = data)
    df.set_index(new_indx, inplace=True)
    # if testing the code
    if sample:
        logger.warning('WARNING: Sampling data-not a full run')
        df = df.sample(n=400, random_state=5)
    logger.info('Shapefile ' + source + ' read into dataframe')
    return df