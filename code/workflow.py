# workflow

import shp2db
import add_demographics
import calcHSSAscores
import subset_database
import removeTableFromDb
import aggregate_scores
import dbToGeoJson

# create table for contracts
print('shp2db...')
shp2db.main()

# subset the database
print('subset_database...')
subset_database.main()

# add the pop>65
print('add_demographics...')
add_demographics.main(('pop_over_65', True))

# calculate the HSSA score
print('calcHSSAscores...')
calcHSSAscores.main()

# add the rest of the demographics
fields = [('pop_female','H76026'),('pop_below_10',True),('pop_color',True), ('pop_total','H76001') ]
for field in fields:
    add_demographics.main(field)

# remove tables from db (compress it)
# print('removeTableFromDb...')
# removeTableFromDb.main()

# aggregate scores
print('aggregate_scores...')
for unit in ['district','neighborhood']:
    aggregate_scores.main(unit)

# convert to json-esque format
print('dbToGeoJson')
dbToGeoJson.main()