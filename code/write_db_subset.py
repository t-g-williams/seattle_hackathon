import sqlite3

db_in = '../query_results/combined-data_5km_with_hssa.db'
db_out = '../query_results/sea_API.db'
tbl_to_save = ['orig', 'contracts', 'destsubset']

# connect to the database
db = sqlite3.connect(db_fn)
cursor = db.cursor()

# select the tables to save


# write

