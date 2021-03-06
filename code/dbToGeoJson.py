# Create a geojson file using the coordinates from a shapefile and data from a database

import shapefile
import sqlite3
import pandas as pd
import geojson
import json
import pickle
import code
import logging
import os
import generalDBFunctions as db_fns
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

outdir = '../data/frontend_plotting/'
levels = ['district', 'neighborhood', 'block']

def main(level, outdir, db_fn):
    '''
    Create a geojson or geojson-esque file containing shape, demographic and HSSA information
    '''
    logger.info('creating json for {}'.format(level))
    sf_readname = '../data/boundaries/' + level + '_data'
    table_name = level

    if level == 'district':
        id_col_num = 2
        db_id_col_name = 'area_id'

    if level == 'neighborhood':       
        id_col_num = 4
        db_id_col_name = 'area_id'

    if level == 'block':
        db_fn =  '../query_results/sea_5km.db'
        id_col_num = 7
        db_id_col_name = 'orig_id'
        table_name = 'orig'

    # below here, nothing should need to be changed

    # get the shapefile data
    shape_data = getShapeData(sf_readname, id_col_num)

    # import database table (data)
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()
    c_names = db_fns.getColNames(cursor, table_name)
    walk_data = db_fns.getTable(cursor, table_name, list(range(len(c_names))), c_names)
    # set index
    walk_data.set_index(db_id_col_name, inplace=True)

    # link these together, and export gj file
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # create a geojson-esque file
    outname = outdir + level + '_data.json'
    createForApi(shape_data, walk_data, outname)
    # create geojson file
    outname = outdir + level + '_data.gj'
    createGeoJson(shape_data, walk_data, outname)

def getShapeData(sf_readname, id_col_num):
    '''
    get the shape data
    return a dictionary containing key = data from id_col_num and value = coordinates
    '''
    sf = shapefile.Reader(sf_readname)
    shapes = sf.shapes()

    # get shape records
    sfrec = sf.shapeRecords()
    sf_data = {}
    # loop through shapes
    for i in range(len(sfrec)):
        # get the blockid and coords
        block_id = sfrec[i].record[id_col_num]
        block_coords = shapes[i].points
        # create dict
        sf_data[block_id] = block_coords
    
    # add in element for 'all'
    sf_data['all'] = []

    return(sf_data)


def createForApi(shape_data, attr_data, outname):
    '''
    create a geojson with coordinates from shape_data and other information from attr_data
    do this by building up a dictionary
    NOTE: the way this is currently set up, it's not strictly a geojson, but a list of dictionaries
    '''
    gj_list = []
    data_types = list(attr_data.columns.values)

    # add elements to the geometries - loop through the shapes
    for shape_id, coords in shape_data.items():
        # if shape_id == 'all':
            # continue
        #
        shape_dict = {}
        shp_data = attr_data.ix[str(shape_id)]
        #
        # create dictionary of the data
        shp_data_dict = {}
        for type in data_types:
            shp_data_dict[type] = shp_data.ix[type]
        shp_data_dict.update({'id' : shape_id})
        #
        # convert coordinates to list of lists
        coords_list = []
        for elem in coords:
            coords_list.append([elem[1] ,elem[0]])
        #
        # add coordinates to dictionary
        shape_dict['coordinates'] = coords_list
        # add the block data to the dictionary
        shape_dict.update(shp_data_dict)
        #
        # append to master list
        gj_list.append(shape_dict)

    # write the json file
    with open(outname, 'w') as file:
        file.write(json.dumps(gj_list))

def createGeoJson(shape_data, attr_data, outname):
    '''
    create a geojson with coordinates from shape_data and other information from attr_data
    do this by building up a dictionary
    '''
    gj_dict = {}
    gj_dict['type'] = 'FeatureCollection'
    gj_dict['features'] = [{'geometry' : {'type' : 'GeometryCollection', 'geometries' : []}}]
    data_types = list(attr_data.columns.values)

    for shape_id, coords in shape_data.items():
        if shape_id == 'all':
            continue
        # find matching row in attr_data
        shape_data = attr_data.ix[shape_id]
        
        shape_data_dict = {}
        for type in data_types:
            shape_data_dict[type] = shape_data.ix[type]
        
        # convert coordinates to list of lists
        coords_list = []
        for elem in coords:
            coords_list.append([elem[0] ,elem[1]])

        # add to the dictionary
        shape_dict = {'type' : 'Polygon', 'coordinates' : coords_list}
        # add in the block data, 'HSSAscore' : hssa_score}
        shape_dict.update(shape_data_dict)
        # append to master dictionary
        gj_dict['features'][0]['geometry']['geometries'].append(shape_dict)

    # write the json file
    with open(outname, 'w') as file:
        file.write(json.dumps(gj_dict))

if __name__ == '__main__':
    for level in levels:
        main(level, outdir)