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
    db_fn =  '../query_results/combined-data_5km_contracts.db'
    db_fn =  '../query_results/sample_results_2.db'
    max_dur = 30*60 # 30 minutes

    subsetDatabase(db_fn, max_dur)


def subsetDatabase(db_fn, max_dur):
    # create connection to the database
db = sqlite3.connect(db_fn)
cursor = db.cursor()

    # create a dataframe with only the relevant O-D pairs
    createSubsetDataframe(cursor, max_dur)

    # calculate walk scores for origins
    calcWalkScores(cursor, max_dur)

    # # add origin lat and lon (for plotting)
    # od_pairs = addLatLon(od_pairs, cursor, 'orig')

    # # add the contract data
    # od_pairs = addContractData(od_pairs, cursor, 'contracts')

    # # add demographic data
    # dem_ids = {'pop' : 3, 'pop_over_65' : 4}
    # od_pairs = addDemographicData(od_pairs, cursor, 'orig', dem_ids)


def calcWalkScores(cursor, max_dur):
    # calculate the walk score for each origin

    # get np.DataFrame of orig ids
    orig_ids = getTable(cursor, 'orig', [0], ['orig_id'])
    scores_dict = {}
    # Loop through each origin id
    for i in range(len(orig_ids)):
        if i % 100 == 0:
            print('i = {} / {}'.format(i, len(orig_ids)))
        # find all services within 30min of this orig
        services_pd = getVendorsForOrig(orig_ids.ix[i][0], cursor)
        # loop through the services
        for j in range(services_pd.shape[0]):
            # get the duration to this service
            tmp = cursor.execute('''SELECT walking_time FROM destsubset 
                WHERE dest_id={} AND orig_id={}'''
                .format(services_pd.ix[j, 'dest_id'], orig_ids.ix[i][0]))
            duration = tmp.fetchall()
            # add to data frame
            services_pd.ix[j, 'walking_time'] = duration [0][0]
        # CALCULATE WALKING SCORE
        score = calcHSSAScore(services_pd, cursor, max_dur)
        scores_dict[orig_ids.ix[i][0]] = score
    # add scores to database



def calcHSSAScore(services, cursor, max_dur):
    # Calculate the HSSA score for a given origin
    # Note: this code is adapted from Logan Noel's code
    # https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
    WEIGHTS = [.1, .25, .5, .75, 1]
    weight_dict = {}
    score = 0
    for i in range(services.shape[0]):
        # cat = VendorLookup(cursor, services.ix[i, 'ContractNo'], 'Project')
        cat = services.ix[i, 'Project']
        if cat not in weight_dict:
            weight_dict[cat] = WEIGHTS
        if len(weight_dict[cat]) > 0:
            variety_weight = weight_dict[cat].pop()
        else:
            variety_weight = 0
        distance_weight = linearDecayFunction(services.ix[i, 'walking_time'], max_dur)
        # calculate score
        score += variety_weight * distance_weight * services.ix[i,'TotalBudgt']
    return(score)


def linearDecayFunction(time, upper):
    # penalty function for distance
    # taken from https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
    upper = float(upper)
    time = float(time)
    if time > upper:
        return 0
    else:
        return (upper - time) / upper

# def VendorLookup(cursor, id, kind):
#     # look up the value for a specific record, such as Project or TotalBudgt
#     # Note: this code is adapted from Logan Noel's code
#     # https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
# query = "SELECT {} FROM contracts WHERE ContractNo is {}".format(kind, id)
# data = cursor.execute(query).fetchone()
#     return(data)
    

def getVendorsForOrig(orig_id, cursor):
    # get all of the vendors within reach of a given origin point
    # note - doesn't actually get the duration (creates a column with 'None')
    tmp = cursor.execute('''SELECT * FROM contracts 
        WHERE dest_id IN 
        (SELECT dest_id FROM destsubset WHERE orig_id={})'''
        .format(orig_id))
    services_tuple = tmp.fetchall()
    # convert to pandas data frame
    services_list = [x for x in services_tuple]
    services_pd = pd.DataFrame(services_list, columns=getColNames(cursor, 'contracts'))
    # add column for duration
    services_pd['walking_time'] = None 
    return(services_pd)   

def createSubsetDataframe(cursor, max_dur):
    # create a pandas dataframe containing the O-D pairs for destinations that contain services
    # and adds it to the database

    # # get list of dest id's that contain the services of interest
    # tmp = cursor.execute("SELECT dest_id FROM contracts")
    # service_dest_ids_tuple = tmp.fetchall()
    # service_dest_ids = [x[0] for x in service_dest_ids_tuple]

    # # filter the database to O-D pairs with duration < specified time
    # tmp = cursor.execute("SELECT * FROM origxdest WHERE walking_time < {}".format(max_dur))

    tmp = cursor.execute('''SELECT * FROM walking 
    WHERE duration < {}
    AND dest_id IN (SELECT dest_id FROM contracts)'''.format(max_dur))
    #
    od_pairs = tmp.fetchall()
    #
    # create pandas dataframe
    data_list = [[row[0], row[1], row[2]] for row in od_pairs]
    od_pairs = pd.DataFrame(data_list, columns=['orig_id', 'dest_id', 'walking_time'])
    # od_pairs = od_pairs.set_index('dest_id', inplace=True)

        # filter the 'origxdest' table to just these destinations
        # od_pairs_subset = od_pairs.ix[service_dest_ids]

    # write this as a table in the database...
    # strings
    cols_str = "orig_id VARCHAR (15), dest_id VARCHAR (15), walking_time INT"
    col_names = ['orig_id', 'dest_id', 'walking_time']
    # convert index back to column and format data frame
    # od_pairs_subset['dest_id'] = od_pairs_subset.index
    # od_pairs_subset = od_pairs_subset[['orig_id', 'dest_id', 'walking_time']]
    # add to data base
    addPdToDb(od_pairs, cursor, 'destsubset', cols_str, col_names)
    db.commit()

def addPdToDb(d_frame, cursor, new_table_name, cols_str, col_names):
    # add a pandas dataframe (d_frame) to a database (db)
    # NOTE: this code is not generalizable (it adds the 3rd column as an int)
    # create new table
    add_table_str = "CREATE TABLE {}({})".format(new_table_name, cols_str)
    db.execute(add_table_str)
    #
    # add data
    add_data_str = "INSERT INTO {}({}) VALUES(?,?,?)".format(new_table_name, ', '.join(col_names))
    for i in range(d_frame.shape[0]):
        # cursor.execute(add_data_str, (d_frame.ix[i,:]))
        cursor.execute(add_data_str, (d_frame.ix[i,0],d_frame.ix[i,1],int(d_frame.ix[i,2])))

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

def addDemographicData(d_frame, cursor, table_name, dem_ids):
    # add demographic data to the data frame
    # dem_ids is a dictionary with key=column name and value=column number

    # get the demographic data
    # NEED TO WAIT UNTIL AGE / POPULATION DATA ADDED TO TABLE
    for (col_name, col_num) in dem_names:
        dem_pd = getTable(cursor, 'orig', [0, col_num], ['orig_id', col_name])

    # merge with the data frame
    d_frame_combined = pd.merge(d_frame, dem_pd, left_on='orig_id', right_on='orig_id')

    return(d_frame_combined)

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
    tmp.fetchone()
    nmes = [description[0] for description in tmp.description]
    # print(nmes)
    return(nmes)

# scratch

# temp = getTable(db, 'destsubset', [0,1,2], ['orig_id', 'dest_id', 'walking_time'])

# id_list = od_pairs.index.tolist()
# for i in service_dest_ids:
#    if i in id_list:
#        print ('phew')

# def calculateAccessScore():

#     # remove all O-D pairs with no destination or >30min

#     for orig in origs:
#         # find all dests with services

#         score = 0
#         for each dest:
#             for each service in dest:
#                 score += new_score

#     # option A
#     for each orig:
#         select from table where dur < 30 AND orig == orig AND (dest in service_dest)

#     # option B
#     select from table where dur < 30 and (dest in service_desrt)
#     for each orig:
#         select from new_table hwere orig == orig