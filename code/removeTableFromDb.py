# remove a table from the database

import sqlite3
import pandas as pd
import subset_database_to_dests as sbs
def main():
    # connect to databases
    db_fn = '../query_results/combined-data_5km_with_hssa.db'
    db = sqlite3.connect(db_fn)
    cursor = db.cursor()
    db_fn2 = '../query_results/sea_API.db'
    db2 = sqlite3.connect(db_fn2)

    # # remove tables
    # tables_to_remove = ['origxdest', 'dest', 'walking', 'contracts']

    # for tbl in tables_to_remove:
    #     db.execute("DROP TABLE {}".format(tbl))
    #     print('dropped {}'.format(tbl))

    # # write to new db

    # convert to pandas
    table_name = 'orig'
    c_names = getColNames(cursor, table_name)
    orig_pd = sbs.getTable(db, table_name, range(len(c_names)), c_names)
    print('got table orig')
    orig_pd.to_sql(table_name, con=db2)

    table_name = 'destsubset'
    c_names = getColNames(cursor, table_name)
    orig_pd = sbs.getTable(db, table_name, range(len(c_names)), c_names)
    print('got table destsubset')
    orig_pd.to_sql(table_name, con=db2)

    table_name = 'contracts'
    c_names = getColNames(cursor, table_name)
    orig_pd = sbs.getTable(db, table_name, range(len(c_names)), c_names)
    print('got table contracts')
    orig_pd.to_sql(table_name, con=db2)

    # write to database

    print('dtabase created')
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
    main()

# db.commit()
# db.close()