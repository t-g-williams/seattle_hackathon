'''
Calculate the HSSA score for every origin.
Calculate the funding allocated to every origin (investment).
Inputs:     Database containing:
                - origin-destination pairs (table)
                - a subset of the destinations that contain services of interest (table)
            Maximum duration of walking
Output:     Table containing O-D pairs only for the destinations of interest
'''

import pandas as pd
import numpy as np
import sqlite3
import code
import logging
import generalDBFunctions as db_fns
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# file names and parameters
db_fn =  '../query_results/sea_5km.db'
max_dur = 30*60 # 30 minutes
dem_col_name = 'pop_over_65'

def main(dem_fn, db_fn, dem_col_name):
    # create connection to the database
    logger.info('subsetting the database')
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # calculate walk scores and investment for origins
    calcWalkScoresAndInvestment(cursor, db, max_dur, dem_col_name)

    db.close()

def calcWalkScoresAndInvestment(cursor, db, max_dur, dem_col_name):
    '''
    calculate the walk score for each origin, and the funding allocated to each origin (investment)
    dem_col_name is the column name in the database of the demographic to be used for the investment calculations
    '''
    logger.info('calculating walk scores')
    # get np.DataFrame of orig ids
    orig_ids = db_fns.getTable(cursor, 'orig', [0, -1], ['orig_id', 'pop_over_65'])

    # initialize dictionaries: HSSA scores, contracts for each origin, and contract allocation
    scores_dict = {}
    orig_contracts = {}
    contract_per_cap = {}
    contract_data = db_fns.getTable(cursor, 'contracts', [0, 3], ['ContractNo', 'TotalBudgt'])
    for i in range(contract_data.shape[0]):
        contract_per_cap[contract_data.ix[i,'ContractNo']] = {'amt' : contract_data.ix[i,'TotalBudgt'], 'ppl' : 0}

    # Loop through each origin id
    for i in range(orig_ids.shape[0]):
        if i % 100 == 0:
            print('i = {} / {}'.format(i, orig_ids.shape[0]))
        # find all services for this orig
        services_pd = getVendorsForOrig(orig_ids.ix[i, 'orig_id'], cursor).drop_duplicates()
        # initialize contract list for orig i
        orig_contracts[orig_ids.ix[i,'orig_id']] = {'contracts' : [], 'pop' : orig_ids.ix[i, dem_col_name]}
        # loop through the services
        for j in range(services_pd.shape[0]):
            # get the duration to this service
            query_str = '''SELECT walking_time FROM destsubset 
                WHERE dest_id={} AND orig_id={}'''.format(services_pd.ix[j, 'dest_id'], orig_ids.ix[i, 'orig_id'])
            tmp = cursor.execute(query_str)            
            duration = tmp.fetchone()

            # add to data frame
            services_pd.ix[j, 'walking_time'] = duration[0]
            # add origin pop to the services funding count
            contract_per_cap[services_pd.ix[j, 'ContractNo']]['ppl'] += orig_ids.ix[i,'pop_over_65']
            # add contract id to the origin's contracts
            orig_contracts[orig_ids.ix[i,'orig_id']]['contracts'].append(services_pd.ix[j, 'ContractNo'])
        # CALCULATE WALKING SCORE FOR ORIG
        score = calcHSSAScore(services_pd, cursor, max_dur)
        scores_dict[orig_ids.ix[i,'orig_id']] = {'HSSA' : score}
 
    # calculate per capita spending for each contract
    contract_per_cap = calcPerCapSpending(contract_data, contract_per_cap)
    # calculate spending per origin (update the scores dictionary with this data)
    scores_dict = calcOrigFunding(orig_contracts, contract_per_cap, scores_dict)

    
    # normalize the scores
    HSSAs = [val['HSSA'] for val in scores_dict.values()]
    investments = [val['investment'] for val in scores_dict.values()]
    scores_pd = pd.DataFrame({'orig_id' : list(scores_dict.keys()), 'HSSAscore' : HSSAs, 'investment' : investments})
    scores_pd['HSSAscore'] = 100 * scores_pd['HSSAscore'].divide(max(scores_pd['HSSAscore']))
    logger.info('...normalized the scores')
    
    # add scores to database
    code.interact(local=locals()) 
    WriteDB(scores_pd, db, 'investment')
    WriteDB(scores_pd, db, 'HSSAscore')
    db.commit()

def calcOrigFunding(orig_contracts, contract_per_cap, scores_dict):
    ''' 
    calculate the amount of funding (per capita) to be apportioned to each origin
    using the contracts that each orign has within their walkshed
    and the per capita funding of each service
    add this to scores_dict
    '''
    output_dict = {}
    for orig_id, orig_data in orig_contracts.items():
        orig_spending = 0
        for contract_id in orig_data['contracts']:
            per_cap_spend = contract_per_cap[contract_id]['per_cap']
            orig_spending += per_cap_spend * orig_data['pop']
        scores_dict[orig_id].update({'investment' : orig_spending})
    return(scores_dict)

def calcPerCapSpending(contract_data, contract_per_cap):
    # for each contract, create key for per capita spending and add to dictionary
    for i in range(contract_data.shape[0]):
        dict_i = contract_per_cap[contract_data.ix[i,'ContractNo']]
        # calculate per capita spending
        if dict_i['ppl']:
            d_per_cap = {'per_cap' : dict_i['amt'] / dict_i['ppl']}
        else:
            d_per_cap = {'per_cap' : 0}     
        dict_i.update(d_per_cap)
    return(contract_per_cap)

def WriteDB(df, db, col_name):
    '''
    Add table to db
    '''
    logger.info('Writing to DB')

    cursor = db.cursor()
    # add column
    add_col_str = "ALTER TABLE orig ADD COLUMN {} REAL".format(col_name)
    db.execute(add_col_str) 

    for i in range(len(df)):
        add_data_str = "UPDATE orig SET {} =(?) WHERE orig_id=(?)".format(col_name)
        value = df.iloc[i][col_name]
        idx = df.iloc[i]['orig_id']
        db.execute(add_data_str, (value, idx))

    db.commit()

def calcHSSAScore(services, cursor, max_dur):
    ''' 
    Calculate the HSSA score for a given origin
    Note: this code is adapted from Logan Noel's code
    https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
    '''
    WEIGHTS = [.1, .25, .5, .75, 1]
    weight_dict = {}
    score = 0
    for i in range(services.shape[0]):
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
    '''
    penalty function for distance
    taken from https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
    '''
    upper = float(upper)
    time = float(time)
    if time > upper:
        return 0
    else:
        return (upper - time) / upper

def getVendorsForOrig(orig_id, cursor):
    '''
    get all of the vendors within reach of a given origin point
    note - doesn't actually get the duration (creates a column with 'None')
    '''
    tmp = cursor.execute('''SELECT * FROM contracts 
        WHERE dest_id IN 
        (SELECT dest_id FROM destsubset WHERE orig_id={})'''
        .format(orig_id))
    services_tuple = tmp.fetchall()
    # convert to pandas data frame
    services_list = [x for x in services_tuple]
    services_pd = pd.DataFrame(services_list, columns=db_fns.getColNames(cursor, 'contracts'))
    # add column for duration
    services_pd['walking_time'] = None 
    return(services_pd)   

if __name__ == '__main__':
    main(dem_fn, db_fn, dem_col_name)