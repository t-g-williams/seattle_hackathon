# remove a table from the database

import sqlite3
import pandas as pd
import subset_database_to_dests as sbs

# connect to database
db_fn = '../query_results/combined-data_5km.db'
db = sqlite3.connect(db_fn)
cursor = db.cursor()

# # remove tables
# tables_to_remove = ['origxdest', 'dest', 'walking', 'contracts']

# for tbl in tables_to_remove:
#     db.execute("DROP TABLE {}".format(tbl))
#     print('dropped {}'.format(tbl))

# # write to new db

# convert to pandas
orig_pd = sbs.getTable(db, 'orig', [0,1,2,3,4,5], ['orig_id', 'orig_lon', 'orig_lat', 'pop', 'pop_over_65' ,'HSSAscore'])
print('got table')

# write to database
db_fn2 = '../query_results/sea_5km_orig.db'
db2 = sqlite3.connect(db_fn2)
print(db2)
orig_pd.to_sql('orig', con=db2)
print('made sql?')
db2.commit()
db2.close()

# db.commit()
# db.close()