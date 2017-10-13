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

def main(db_fn, dem_col_name, max_dur):
    '''
    calculate the walk score for each origin, and the funding allocated to each origin (investment)
    dem_col_name is the column name in the database of the demographic to be used for the investment calculations
    '''
    logger.info('calculating HSSA scores')
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()

    # get pd.DataFrame of orig ids and population of interest
    c_names = db_fns.getColNames(db, 'orig')
    dem_c_id = np.where(np.array(c_names) == dem_col_name)[0][0]
    orig_ids = db_fns.getTable(cursor, 'orig', [0, dem_c_id], ['orig_id', dem_col_name])

    # initialize dictionaries: HSSA scores, contracts for each origin, and contract allocation
    scores_dict = {}
    orig_contracts = {}
    contract_per_cap = {}

    # Get and normalize contract data
    contract_data = normalizeContractData(cursor)
    # add this into contract per cap dictionary
    for i in range(contract_data.shape[0]):
        contract_per_cap[contract_data['ContractNo'].iloc[i]] = {'amt' : contract_data.ix[i,'TotalBudgtOriginal'], 'ppl' : 0}

    # Loop through each origin id
    for i in range(orig_ids.shape[0]):
        orig_id = orig_ids.orig_id.iloc[i]
        if i % 100 == 0:
            print('i = {} / {}'.format(i, orig_ids.shape[0]))

        # find all services for this orig
        services_pd = getVendorsForOrig(orig_id, cursor).drop_duplicates()
        # initialize contract list for orig i (as well as temporary dictionary)
        orig_contracts[orig_id] = {'cons' : [], 'pop' : orig_ids[dem_col_name].iloc[i]}
        orig_cons_temp = {'contracts' : {}, 'pop' : orig_ids[dem_col_name].iloc[i]}
        # create dict to store the distance to each unique contract id 
        # - we will just want to store the closest for each
        for c_no in services_pd.ContractNo:
            orig_cons_temp['contracts'].update({c_no : {'min_dur' : 99999, 'j' : -1}})

        # add origin pop to the services funding count - i.e. these people are using this contract
        for con_id in orig_cons_temp['contracts'].keys():
            contract_per_cap[con_id]['ppl'] += orig_ids[dem_col_name].iloc[i]
            orig_contracts[orig_id]['cons'].append(con_id) # append to master list
        # get the minimum distance to each unique contract for this origin
        
        closest_services_pd = getMinContractDists(cursor, orig_id, services_pd, orig_cons_temp)
        
        # CALCULATE WALKING SCORE FOR ORIG
        score = calcHSSAScore(closest_services_pd, cursor, max_dur)
        scores_dict[orig_id] = {'HSSA' : score}
 
    # calculate per capita spending for each contract
    contract_per_cap = calcPerCapSpending(contract_data, contract_per_cap)
    # calculate spending per origin (update the scores dictionary with this data)
    scores_dict = calcOrigFunding(orig_contracts, contract_per_cap, scores_dict)

    # extract the scores
    HSSAs = [val['HSSA'] for val in scores_dict.values()]
    investments = [val['investment'] for val in scores_dict.values()]
    scores_pd = pd.DataFrame({'orig_id' : list(scores_dict.keys()), 'HSSAscore' : HSSAs, 'investment' : investments})
    
    scores_pd['HSSAscore'] = 100 * scores_pd['HSSAscore'].divide(max(scores_pd['HSSAscore']))
    logger.info('...normalized the scores')
    
    # add scores to database 
    WriteDB(scores_pd, db, 'investment')
    WriteDB(scores_pd, db, 'HSSAscore')
    db.commit()


def normalizeContractData(cursor):
    '''
    Fetch the contracts data
    Normalize the spending amounts for each contract id
    Adapted from code at:
    https://github.com/GeoDaCenter/contracts/blob/master/analytics/ScoreModel.py
    '''
    contract_data = db_fns.getTable(cursor, 'contracts', [0, 3], ['ContractNo', 'TotalBudgt'])
    contract_data.set_index('ContractNo', inplace=True)

    # convert to dictionary, adding together the amount of any duplicate contract numbers
    v_amt_table = {}
    for site_id, amount in contract_data.iterrows():
        if site_id in v_amt_table.keys():
            v_amt_table[site_id] += amount[0]
        else:
            v_amt_table[site_id] = amount[0]

    all_contracts = list(v_amt_table.values())

    sd = np.nanstd(all_contracts)
    mean = np.nanmean(all_contracts)
    
    for vend_id, amount in v_amt_table.items():
        v_amt_table[vend_id] = (amount - mean) / sd
    
    min_amt = np.nanmin(list(v_amt_table.values()))

    for vend_id, amount in v_amt_table.items():
        v_amt_table[vend_id] += abs(min_amt)    
    
    # convert to pandas dataframe
    return_table = pd.DataFrame({'ContractNo' : list(v_amt_table.keys()), 'TotalBudgt' : list(v_amt_table.values()),
        'TotalBudgtOriginal' : all_contracts})

    return return_table

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
    services_pd['walking_time'] = -1 
    return(services_pd)   

def getMinContractDists(cursor, orig_id, services_pd, orig_cons_temp):
    '''
    For a single origin, get the minimum distance to each of the contracts it has access to
    '''
    j_to_rm = []
    # loop through the services
    for j in range(services_pd.shape[0]):
        # get the duration to this service
        query_str = '''SELECT walking_time FROM destsubset 
            WHERE dest_id={} AND orig_id={}'''.format(services_pd.dest_id.iloc[j], orig_id)
        tmp = cursor.execute(query_str)            
        duration = tmp.fetchone()

        # add to data frame - this gives a warning but it works
        services_pd.walking_time.iloc[j] = duration[0]

        # update the dictionary entry for this contract number - select minimum distance
        con_id = services_pd.ContractNo.iloc[j]

        prev_min_dur = orig_cons_temp['contracts'][con_id]['min_dur']
        if duration[0] < prev_min_dur:
            # save this distance
            orig_cons_temp['contracts'][con_id]['min_dur'] = duration[0]
            # remove the old j
            j_to_rm += [orig_cons_temp['contracts'][con_id]['j']] if prev_min_dur != 99999 else []
            # add this j to the dictionary
            orig_cons_temp['contracts'][con_id]['j'] = j
        else:
            j_to_rm += [j]

    # remove all unnecessary j's
    return services_pd.drop(services_pd.index[j_to_rm])

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
        cat = services.Project.iloc[i]
        if cat not in weight_dict:
            weight_dict[cat] = WEIGHTS
        if len(weight_dict[cat]) > 0:
            variety_weight = weight_dict[cat].pop()
        else:
            variety_weight = 0
        distance_weight = linearDecayFunction(services.walking_time.iloc[i], max_dur)
        # calculate score
        score += variety_weight * distance_weight * services.TotalBudgt.iloc[i]
    return(score)

def calcPerCapSpending(contract_data, contract_per_cap):
    '''
    for each contract, create key for per capita spending and add to dictionary
    '''
    for i in range(contract_data.shape[0]):
        dict_i = contract_per_cap[contract_data.ContractNo.iloc[i]]
        # calculate per capita spending
        if dict_i['ppl'] > 0:
            d_per_cap = {'per_cap' : dict_i['amt'] / dict_i['ppl']}
        else:
            d_per_cap = {'per_cap' : 0}     
        dict_i.update(d_per_cap)
    return(contract_per_cap)

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
        
        for contract_id in orig_data['cons']:
            per_cap_spend = contract_per_cap[contract_id]['per_cap']
            orig_spending += per_cap_spend * orig_data['pop']

        scores_dict[orig_id].update({'investment' : orig_spending})
    return(scores_dict)

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

if __name__ == '__main__':
    main(db_fn, dem_col_name, max_dur)