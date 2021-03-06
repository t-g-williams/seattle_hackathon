'''
Workflow to generate data for the front-end
Code developed at the Seattle hackathon by Tom logan and Tim Williams

INPUT: A database containing tables: 
- orig (origin points, e.g. blockgroups)
- dest (destination points, e.g. blockgroups (in the case of blockgroups, this is identical to orig)
- origxdest (walking time between origs and dests)
- walking
...
OUTPUT: 3x geoJson-like text files containing:
- results at 3 different spatial levels (block, district, neighborhood)
    - coordinates of each polygon
    - demographic info for each polygon
    - HSSA scores and funding allocation

NOTE: While this is meant to be generalizable, please check through each script before changing anything
'''

import shp2db
import add_demographics
import calcHSSAscores
import subset_database
import duplicateDB
from shutil import copyfile
import aggregate_scores
import dbToGeoJson
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#### file names and parameters ####
# input data (required)
shp_fn = '../data/seattle_contracts/seattle-contracts-geocoded.shp'
block_fn = '../data/block_data/sea_blocks_wgs84.shp'
db_master_fn = '../query_results/new_hssa/combined-data_5km_master.db' # this is the input database name
dem_fn = '../data/demographic/nhgis0002_ds172_2010_block.csv'

# other intermediate inputs and outputs
db_fn = '../query_results/new_hssa/sea_5km.db' # this database will be generated
copy_fn = '../query_results/new_hssa/sea_5km_after_hssa.db'
max_dur = 30*60 # 30 minutes
dem_field_for_hssa = ('pop_over_65', True)
demographic_fields = [('pop_female','H76026'),('pop_below_10',True),('pop_color',True), ('pop_total','H76001') ]
db_api_subset_name = '../query_results/new_hssa/sea_API.db'
tables_for_api_subset = ['orig', 'contracts', 'destsubset']
aggregate_levels = ['district', 'neighborhood']
values_for_aggregate = ['pop_total', 'pop_over_65', 'pop_below_10', 'pop_female', 'pop_color', 'investment']
scores = ['score0_49', 'score50_69','score70_89','score90_100']
aggregate_db_fn = '../query_results/new_hssa/sea_boundaries.db'
json_outdir = '../data/frontend_plotting/'


# create table for contracts
logger.info('shp2db...')
shp2db.main(shp_fn, block_fn, db_fn, db_master_fn)

# subset the database (keep only the O-D pairs for destinations with contract points in them)
logger.info('subset_database...')
subset_database.main(db_fn, max_dur)

# add the pop>65 count for each block
logger.info('add_demographics...')
add_demographics.main(dem_field_for_hssa, dem_fn, db_fn)

# calculate the HSSA scores and funding allocation
logger.info('calcHSSAscores...')
calcHSSAscores.main(db_fn, dem_field_for_hssa[0], max_dur)
copyfile(db_fn, copy_fn)

# add the rest of the demographics
for field in demographic_fields:
    add_demographics.main(field, dem_fn, db_fn)

# remove tables from db (compress it)
print('removeTableFromDb...')
duplicateDB.main(db_fn, db_api_subset_name, tables_for_api_subset)

# aggregate scores to district and neighborhood
logger.info('aggregate_scores...')
for unit in aggregate_levels:
    aggregate_scores.main(unit, scores, values_for_aggregate, block_fn, db_fn, aggregate_db_fn)


# convert to json-esque format
logger.info('dbToGeoJson...')
# NOTE: This is not generalizable if adding new geographies
all_levels = aggregate_levels #+ ['block']
for level in all_levels:
    dbToGeoJson.main(level, json_outdir, aggregate_db_fn)


