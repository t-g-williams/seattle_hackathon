# remove a table from the database

import sqlite3
import pandas as pd
import logging
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
        c_names = getColNames(cursor, tbl)
        orig_pd = sbs.getTable(db, tbl, range(len(c_names)), c_names)
        # write to database
        orig_pd.to_sql(tbl, con=db2)

    logger.info('new database created')
    db2.commit()
    db2.close()
    db.close()


def getColNames(cursor, table_name):
    # get the column names of a given table in a database
    tmp = cursor.execute("SELECT * FROM {}".format(table_name))
    tmp.fetchone()
    nmes = [description[0] for description in tmp.description]
    return(nmes)

if __name__ == '__main__':
    main(db_in, db_out, tables_to_keep)

# db.commit()
# db.close()