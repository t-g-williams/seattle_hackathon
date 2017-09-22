# Inputs:     Database containing:
#                 - origin-destination pairs (table)
#                 - a subset of the destinations that contain services of interest (table)
#             Maximum duration of walking
# Output:     Table containing O-D pairs only for the destinations of interest

import pandas as pd
import numpy as np
import sqlite3

def main():
    # file names and parameters
    db_fn =  '../query_results/sample_results_2.db'
    max_dur = 30*60 # 30 minutes

    subsetDatabase(db_fn, max_dur)


def subsetDatabase(db_fn, max_dur):
    # create connection to the database
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # create a dataframe with only the relevant O-D pairs
    od_pairs = createSubsetDataframe(cursor, max_dur)

    # add origin lat and lon (for plotting)
    od_pairs = addLatLon(od_pairs, cursor, 'orig')

    # add the contract data
    od_pairs = addContractData(od_pairs, cursor, 'contracts')



def createSubsetDataframe(cursor, max_dur):
    # create a pandas dataframe containing the O-D pairs for destinations that contain services

    # get list of dest id's that contain the services of interest
    tmp = cursor.execute("SELECT dest_id FROM contracts")
    service_dest_ids_tuple = tmp.fetchall()
    service_dest_ids = [x[0] for x in service_dest_ids_tuple]

    # filter the database to O-D pairs with duration < specified time
    tmp = cursor.execute("SELECT * FROM origxdest WHERE walking_time < {}".format(max_dur))


    # tmp = cursor.execute('''SELECT * FROM origxdest 
    # WHERE walking_time < {}
    # AND dest_id IN (SELECT dest_id FROM contracts)'''.format(max_dur))

    od_pairs = tmp.fetchall()

    # create pandas dataframe
    data_list = [[row[0], row[1], row[2]] for row in od_pairs]
    od_pairs = pd.DataFrame(data_list, columns=['orig_id', 'dest_id', 'walking_time'])
    od_pairs.set_index('dest_id', inplace=True)

    # filter the 'origxdest' table to just these destinations
    od_pairs_subset = od_pairs.ix[service_dest_ids]

    return(od_pairs_subset)

def addLatLon(d_frame, cursor, table_name):
    # add lat and lon columns to the data
    # retrieve the lat and lon from table_name
    # match the lat/lon to the d_frame using 'orig_id' column name
    # NOTE: this assumes there are three columns in 'table_name' corresponding to id, lon, and lat

    # get the lat/lon data
    lat_lon_pd = getTable(cursor, table_name, [0,1,2], ['id', 'lon', 'lat'])
    # tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    # tuple_data = tmp.fetchall()
    # # turn into pandas dataframe
    # data_list = [[row[0], row[1], row[2]] for row in tuple_data]
    # lat_lon_pd = pd.DataFrame(data_list, columns=['id', 'lon', 'lat'])
    lat_lon_pd.set_index('id', inplace=True)

    # match to the input dataframe
    # CHECK THIS! -- does it work with 'id' as index
    d_frame_combined = pd.merge(d_frame, lat_lon_pd, left_on='orig_id', right_on='id')

    return(d_frame_combined)


def addContractData(d_frame, cursor, table_name):
    # Add info about the contracts to the data frame
    # get the contract data
    contract_pd = getTable(cursor, table_name, [4,1,3], ['dest_id', 'Project', 'TotalBudgt'])
    
    # some destinations may have multiple contracts -- identify these and add together
    # get unique contract dests
    contract_dests = np.unique(contract_pd.loc[:, 'dest_id'])
    # creat pd dataframe to be filled out
    nans = [float('NaN')]*len(contract_dests)
    pd_data_dict = {'dest_id' : contract_dests, 'tot_budget' : nans, 'con_names' : nans}
    comb_unique_contracts = pd.DataFrame(pd_data_dict)
    comb_unique_contracts.set_index('dest_id', inplace=True)
    for dest in contract_dests:
        # find matching ids
        ids = np.where(contract_pd.loc[:,'dest_id'] == dest)
        # get total budget and contract names
        comb_unique_contracts.ix[dest, 'tot_budget'] = sum([contract_pd.loc[id,'TotalBudgt'] for id in ids[0]])
        comb_unique_contracts.ix[dest, 'con_names'] = '\n'.join([contract_pd.loc[id, 'Project'] for id in ids[0]])
    
    # match to the data frame
    d_frame_combined = pd.merge(d_frame, comb_unique_contracts, left_index=True, right_index=True)
    return(d_frame_combined)


def addDemographicData(d_frame, cursor, table_name, dem_name):
    # add demographic data to the data frame

    # get the demographic data
    # NEED TO WAIT UNTIL AGE / POPULATION DATA ADDED TO TABLE
    dem_pd = getTable(cursor, 'orig', [0, 3], ['orig_id', 'age'])




# scratch

# id_list = od_pairs.index.tolist()
# for i in service_dest_ids:
#    if i in id_list:
#        print ('phew')

def getTable(cursor, table_name, col_nums, col_names):
    # get table 'table_name' from the database
    # convert to pandas data frame
    # col_nums = a list of column numbers
    # col_names = a list of column names
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tuple_data = tmp.fetchall()
    # convert to pandas dataframe
    data_list = [[row[i] for i in col_nums] for row in tuple_data]
    contract_pd = pd.DataFrame(data_list, columns=col_names)
    return(contract_pd)


def getTabNames(db):
    nms = db.execute("SELECT name FROM sqlite_master WHERE type='table';")
    [print(nm) for nm in nms]

def getColNames(cursor, table_name):
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tmp.fetchall()
    nmes = [description[0] for description in tmp.description]
    print(nmes)

