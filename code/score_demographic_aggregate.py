
import geopandas as gpd
import pandas as pd
import numpy as np
import sqlite3

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    '''
    Create db table for neighborhood or council district
    and count of binned access scores
    '''

    # user variables:
    unit = 'neighborhood'
    var_dtype = [('area_id', 'VARCHAR(15)'),
                ('score0-49', 'INT'),
                ('score50-69', 'INT'),
                ('score70-89', 'INT'),
                ('score90-100', 'INT'),
                ('score_mean', 'REAL'),
                ('investment', 'REAL'),
                ('number_of_services', 'INT'),
                ('pop_total', 'INT'),
                ('pop_over_65', 'INT'),
                ('pop_under_10', 'INT'),
                ('pop_female', 'INT'),
                ('pop_color', 'INT')
                ]

    if unit == 'district':
        unit_id = 'C_DISTRICT' #'HOODS_' 
        dstr_fn = '../data/Council_Districts/Council_Districts.shp' #Neighborhoods/Neighborhoods.shp'
    elif unit == 'neighborhood':
        unit_id = 'HOODS_'
        dstr_fn = '../data/Neighborhoods/Neighborhoods.shp'
    else:
        logger.error('boundary not defined')

    block_fn = '../data/block_data/sea_blocks_wgs84.shp'
    db_fn = '../query_results/sea_5km_origndaries.db'.format(unit)

    # import the areal boundaries
    logger.info('importing data')
    blks = gpd.read_file(block_fn)[['BLOCKID10','geometry']]
    dstrs = gpd.read_file(dstr_fn)
    dstrs = dstrs[[unit_id,'geometry']]

    #Initialize connections to .db
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # Init connect to new database
    # if not os.path.isfile(district_db_fn):
    # open connection
    db_dstr = sqlite3.connect(district_db_fn)
    cursor_dst = db_dstr.cursor()
    # create table
    create_table = 'CREATE TABLE {}(area_id VARCHAR(5), score0_49 INT, score50_69 INT, score70_89 INT, score90_100 INT, mean REAL)'.format(unit)
    # execute and commit
    cursor_dst.execute(create_table)
    db_dstr.commit()

    # score bin upper limits
    bin_lim = np.array([50,70,90,100])

    # loop through
    logger.info('looping {}'.format(unit))
    for i in range(len(dstrs)):
        # which blocks are within poly
        poly = dstrs['geometry'][i]
        district_id = str(dstrs[unit_id][i])
        pts_in = blks.intersects(poly) 
        # BLOCKID10 in
        block_ids = blks['BLOCKID10'][pts_in].tolist()
        # query SQL to return the score and pop_over_65
        query_str = 'SELECT {}, {} FROM {} WHERE orig_id = (?)'.format('HSSAscore','pop_over_65', 'orig')
        result = []
        for i in range(len(block_ids)):
            result.append(cursor.execute(query_str,(block_ids[i],)).fetchone())
        # convert result to dictionary
        pop_total = 0
        score_weight = 0
        bins = [0,0,0,0]
        for (score, pop) in result:
            score = int(score*100)
            # mean
            pop_total += pop
            score_weight += score*pop
            # bin
            idx = np.argmax(bin_lim>score) #next(x[0] for x in bin_lim if x[1] > score)
            bins[idx] += pop
        # mean
        dst_mean = round(score_weight/pop_total if pop_total != 0 else 0, 2)


        # add to SQL
        vals = ['?']*6
        str_to_add = 'INSERT INTO {}(area_id, score0_49, score50_69, score70_89, score90_100, mean)'.format(unit) + ' VALUES(' + ', '.join(vals) + ')'  
        include = tuple([district_id] + bins + [dst_mean])
        cursor_dst.execute(str_to_add, include)
        
    db_dstr.commit()
    db_dstr.close()
    db.close()
    logger.info('COMPLETE')


def ImportData(shp_fn):
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

    return sf



if __name__ == '__main__':
    main()