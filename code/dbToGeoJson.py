# Create a geojson file using the coordinates from a shapefile and data from a database

import shapefile
import sqlite3
import pandas as pd
import geojson
import json
import pickle
import code
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # select aggregation(s)
    levels = ['district', 'neighborhood']

    for level in levels:
        logger.info('Level = {}'.format(level))
        createPlotData(level)

def createPlotData(level):
    # district
    if level == 'district':
        table_name = 'district'
        sf_readname = '../data/boundaries/district_data'
        db_fn =  '../query_results/sea_boundaries.db'
        id_col_num = 2
        db_id_col_name = 'area_id'
        outname = '../data/frontend_plotting/district_data.json'
    
    # neighborhood
    if level == 'neighborhood':
        table_name = 'neighborhood'
        sf_readname = '../data/boundaries/neighborhood_data'
        db_fn =  '../query_results/sea_boundaries.db'
        id_col_num = 4
        db_id_col_name = 'area_id'
        outname = '../data/frontend_plotting/neighborhood_data.json'

    # blocks
    if level == 'block':
        table_name = 'orig'
        sf_readname = '../data/boundaries/block_data'
        db_fn =  '../query_results/sea_5km.db'
        id_col_num = 7
        db_id_col_name = 'orig_id'
        outname = '../data/frontend_plotting/block_data.json'


    # below here, nothing should need to be changed

    # get the shapefile data
    shape_data = getShapeData(sf_readname, id_col_num)

    # import database table (data)
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()
    c_names = getColNames(cursor, table_name)
    walk_data = getTable(cursor, table_name, list(range(len(c_names))), c_names)
    # set index
    walk_data.set_index(db_id_col_name, inplace=True)

    # link these together, and export gj file
    createGeoJson(shape_data, walk_data, outname)


def getShapeData(sf_readname, id_col_num):
    # get the shape data
    # return a dictionary containing key = data from id_col_num and value = coordinates
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


def createGeoJson(shape_data, attr_data, outname):
    # create a geojson with coordinates from shape_data and other information from attr_data
    # do this by building up a dictionary
    # NOTE: the way this is currently set up, it's not strictly a geojson, but a list of dictionaries
    gj_list = []
    data_types = list(attr_data.columns.values)

    # add elements to the geometries - loop through the shapes
    for shape_id, coords in shape_data.items():
        shape_dict = {}
        shp_data = attr_data.ix[str(shape_id)]
        print(shp_data[])
        # create dictionary of the data
        shp_data_dict = {}
        for type in data_types:
            shp_data_dict[type] = shp_data.ix[type]
        shp_data_dict.update({'id' : shape_id})
        
        # convert coordinates to list of lists
        coords_list = []
        for elem in coords:
            coords_list.append([elem[1] ,elem[0]])

        # add coordinates to dictionary
        shape_dict['coordinates'] = coords_list
        # add the block data to the dictionary
        shape_dict.update(shp_data_dict)

        # append to master list
        gj_list.append(shape_dict)

    # write the json file
    import code

    with open(outname, 'w') as file:
        file.write(json.dumps(gj_list))

def getTable(cursor, table_name, col_nums, col_names):
    ''' get table 'table_name' from the database and convert to pandas data frame
    col_nums = a list of column numbers
    col_names = a list of column names
    '''
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tuple_data = tmp.fetchall()
    # convert to pandas dataframe
    data_list = [[row[i] for i in col_nums] for row in tuple_data]
    data_pd = pd.DataFrame(data_list, columns=col_names)
    return(data_pd)

def getColNames(cursor, table_name):
    # get the column names of a given table in a database
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tmp.fetchone()
    nmes = [description[0] for description in tmp.description]
    return(nmes)

if __name__ == '__main__':
    main()