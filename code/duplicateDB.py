# remove a table from the database

import sqlite3
import pandas as pd
import logging
import generalDBFunctions as db_fns
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# connect to databases
db_in = '../query_results/sea_5km.db'
db_out = '../query_results/sea_API.db'
tables_to_keep = ['orig', 'contracts', 'destsubset']

def main(db_in, db_out, tables_to_keep):

    db = sqlite3.connect(db_in)
    cursor = db.cursor()

    db2 = sqlite3.connect(db_out)
    cursor2 = db2.cursor()

    for tbl in tables_to_keep:
        logger.info('{}...'.format(tbl))
        c_names = db_fns.getColNames(cursor, tbl)
        c_names_str = ', '.join(c_names)
        val_str = ', '.join(len(c_names) * '?')

        # add table and columns to new database
        add_table_str = "CREATE TABLE {}({})".format(tbl, c_names_str)
        cursor2.execute(add_table_str)

        # add data to table
        tbl_data = db.execute("SELECT * FROM {}".format())
        print('fetched table')
        i = 0
        for row in tbl_data.fetchall():
            i += 1
            print(i)
            data_str = "INSERT INTO {} VALUES ({})".format(tbl, val_str)
            cols = tuple([k for k in row.keys() if k != 'id'])
            row_data = [row[c] for c in cols]
            cursor2.execute(data_str, row_data)





        orig_pd = db_fns.getTable(db, tbl, range(len(c_names)), c_names)
        # write to database
        orig_pd.to_sql(tbl, con=db2)

    logger.info('new database created')
    db2.commit()
    db2.close()
    db.close()

if __name__ == '__main__':
    main(db_in, db_out, tables_to_keep)

# db.commit()
# db.close()