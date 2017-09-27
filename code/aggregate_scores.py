
import geopandas as gpd
import pandas as pd
import numpy as np
import sqlite3

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# user variables:
scores = ['score0_49', 'score50_69','score70_89','score90_100']
demographics = ['pop_total', 'pop_over_65', 'pop_below_10','pop_female','pop_color', 'investment']
units = ['district','neighborhood']
# file names
block_fn = '../data/block_data/sea_blocks_wgs84.shp'
db_fn = '../query_results/sea_5km.db'
aggregate_db_fn = '../query_results/sea_boundaries.db'


def main(unit, scores, demographics, block_fn, db_fn, aggregate_db_fn):
    '''
    Create db table for neighborhood or council district
    and count of binned access scores
    '''
    var_dtype = [{'area_id': {'dtype': 'VARCHAR(15)', 'val' : ''}},
                {'score0_49': {'dtype':'INT', 'val' : 0}},
                {'score50_69': {'dtype':'INT', 'val' : 0}},
                {'score70_89': {'dtype':'INT', 'val' : 0}},
                {'score90_100': {'dtype':'INT', 'val' : 0}},
                {'score_mean': {'dtype':'REAL', 'val' : 0}},
                # {'number_of_services': {'dtype':'INT', 'val' : 0}},
                {'pop_total': {'dtype':'INT', 'val' : 0}},
                {'pop_over_65': {'dtype':'INT', 'val' : 0}},
                {'pop_below_10': {'dtype':'INT', 'val' : 0}},
                {'pop_female': {'dtype':'INT', 'val' : 0}},
                {'pop_color': {'dtype':'INT', 'val' : 0}},
                {'investment': {'dtype':'REAL', 'val' : 0}},
                ]
    var_dtype_keys = [k  for mydict in var_dtype for k in mydict]
    var_val = {d: i[d]['val'] for (d,i) in zip(var_dtype_keys,var_dtype)}
    var_dtype = {d: i[d]['dtype'] for (d,i) in zip(var_dtype_keys,var_dtype)}
    # get file paths
    unit_id, dstr_fn = GetShpFilePath(unit)

    # import the areal boundaries
    blks, dstrs = ImportData(block_fn, dstr_fn, unit_id)

    #Initialize connections to .db
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # Init connect to new database
    db_dstr = sqlite3.connect(aggregate_db_fn)
    cursor_dst = db_dstr.cursor()

    # create table
    CreateTable(var_dtype, var_dtype_keys, unit, db_dstr, cursor_dst)

    # score bin upper limits
    bin_lim = np.array([5,10,15,100])
    

    # loop through
    logger.info('looping {}'.format(unit))
    for i in range(len(dstrs) + 1):
        # re-init the variables
        var_dict_i = var_val
        if i < len(dstrs):
            # add name to dict
            var_dict_i['area_id'] = str(dstrs[unit_id][i])
            # which blocks are within poly
            poly = dstrs['geometry'][i]
            pts_in = blks.intersects(poly) 
            # BLOCKID10 in
            block_ids = blks['BLOCKID10'][pts_in].tolist()
        else:
            # add name to dict
            var_dict_i['area_id'] = 'all'
            # which blocks are within poly
            block_ids = blks['BLOCKID10'].tolist()
        # query SQL to return the score and pop_over_65
        query_str = 'SELECT HSSAscore, ' + ','.join(demographics) + ' FROM {} WHERE orig_id = (?)'.format('orig')
        results = []
        for j in range(len(block_ids)):
            results.append(cursor.execute(query_str,(block_ids[j],)).fetchone())


        # convert result to dictionary
        pop_total = 0
        score_weight = 0
        bins = [0,0,0,0]
        for result in results:
            score = result[0]
            var_dict_i['pop_total'] += result[1]
            pop = result[2]
            
            var_dict_i['pop_over_65']  += pop
            score = int(score*100)
            # mean
            pop_total += pop
            score_weight += score*pop
            # bin
            idx = np.argmax(bin_lim > score) 
            bins[idx] += pop
            # add demographics
            for k in range(len(demographics)):
                d = demographics[k]
                var_dict_i[d] += result[k + 1]

        # add to dictionary
        
        for j in range(len(scores)): var_dict_i[scores[j]] = bins[j] 
        var_dict_i['score_mean'] = round(score_weight/pop_total if pop_total != 0 else 0, 2)

        # add to SQL: build the command
        vals = ['?'] * len(var_dtype)
        var_names = var_dtype_keys
        str_to_add = 'INSERT INTO {}('.format(unit) + ', '.join(var_names) + ') VALUES(' + ', '.join(vals) + ')'  
        # add to SQL: insert data
        include = tuple([var_dict_i[k] for k in var_dtype_keys])
        cursor_dst.execute(str_to_add, include)
    
    # commit and close dbs
    db_dstr.commit()
    db_dstr.close()
    db.close()
    logger.info('COMPLETE')


def ImportData(block_fn, dstr_fn, unit_id):
    '''
    Read Shapefile and subset by attributes
    '''
    logger.info('importing data')

    # block shapefile
    blks = gpd.read_file(block_fn)[['BLOCKID10','geometry']]

    # politcal boundary
    dstrs = gpd.read_file(dstr_fn)
    dstrs = dstrs[[unit_id,'geometry']]

    return blks, dstrs


def GetShpFilePath(unit):
    '''
    Based on political boundary, get file path
    '''
    if unit == 'district':
        unit_id = 'C_DISTRICT' #'HOODS_' 
        dstr_fn = '../data/Council_Districts/Council_Districts.shp' #Neighborhoods/Neighborhoods.shp'
    elif unit == 'neighborhood':
        unit_id = 'HOODS_'
        dstr_fn = '../data/Neighborhoods/Neighborhoods.shp'
    else:
        logger.error('boundary not defined')

    return unit_id, dstr_fn


def CreateTable(var_dtype, var_dtype_keys, unit, db_dstr, cursor_dst):
    '''
    Create the table with headers
    '''
    # create table
    create_table_str = [ k + ' ' + var_dtype[k] for k in var_dtype_keys] 
    create_table_str = ', '.join(create_table_str)
    create_table = 'CREATE TABLE {}('.format(unit) + create_table_str + ')'
    
    # execute and commit
    cursor_dst.execute(create_table)
    db_dstr.commit()


if __name__ == '__main__':
    for unit in units:
        main(unit, scores, demographics, block_fn, db_fn, aggregate_db_fn)
