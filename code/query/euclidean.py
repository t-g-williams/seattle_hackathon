'''
Calculate the Euclidean distance between all points to inform the querying
'''
import time
import sqlite3
from math import radians, cos, sin, asin, sqrt
import os
import logging
logger = logging.getLogger(__name__)


def calculate(mode, db_fn, db_temp_fn):
    '''
    Generate the euclidean distance for each orig, dest pair.
    '''
    logger.info('Calculating euclidean distances')
    start = time.time()
    
    #Initialize conneciton to .db's
    db = sqlite3.connect(db_temp_fn)
    cursor = db.cursor()
    db1 = sqlite3.connect(db_fn)
    cursor1 = db1.cursor()

    #Add function to cursor and execute query
    db.create_function("haversine", 4, haversine)
    cursor.execute('''SELECT orig_id, dest_id,
    haversine(orig_lon, orig_lat, dest_lon, dest_lat) as euclidean
    FROM orig CROSS JOIN dest''')

    #Iterate over each pair in .db until none remain
    data = cursor.fetchmany(1000)
    while data:
        insert_str = 'INSERT INTO origxdest(orig_id, dest_id, euclidean) VALUES(?, ?, ?)'
        cursor1.executemany(insert_str,(data))
        db1.commit()
        data = cursor.fetchmany(1000)

    #Close up connections and delete temporary data
    db.close()
    db1.close()
    end = time.time()
    if os.path.isfile(db_temp_fn):
        os.remove(db_temp_fn)
        logger.info('Cleaning up...')
    logger.info('Finished calculating distances ({} seconds)'.format(end - start))


def haversine(orig_lon, orig_lat, dest_lon, dest_lat):
    '''
    Calculates the circle distance between two points 
    on the earth (specified in decimal degrees). Implementation from PA3.
    '''
    # convert decimal degrees to radians 
    orig_lon, orig_lat, dest_lon, dest_lat = map(radians, [orig_lon, orig_lat, dest_lon, dest_lat])

    # haversine formula 
    dlon = dest_lon - orig_lon 
    dlat = dest_lat - orig_lat 
    a = sin(dlat/2)**2 + cos(orig_lat) * cos(dest_lat) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 

    # 6367 km is the radius of the Earth
    km = 6367 * c
    m = km * 1000
    m = round(m)

    return m
