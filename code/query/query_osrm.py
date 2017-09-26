# our functions
import database
import euclidean
# pip functions
import requests, csv, time, os.path, logging, multiprocessing, shapefile
import heapq, threading
from progressbar import ProgressBar, Percentage, Bar
from queue import Queue
import pandas as pd
from threading import Thread

import sqlite3
import code

#Logan Noel
#Sept 10, 2017
# Modified by Tom Logan
# Sep 20, 2017

#NB: This is now a semi-complete wrapper for interfacting with OSRM.
#If you have an OSRM server running locally and two shapefiles with
#coordinates, this script will generate the relevant transit times.

#NB: Get help setting up OSRM server at this link:
#https://github.com/Project-OSRM/osrm-backend#quick-start

#Acknowledgements: This code was based on the OSRM wrapper for R by:
#Tom Logan
#Andrew Nisbet
#Tim Williams
#https://github.com/tommlogan/city_access

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# suppress server request calls, so progress is obvious
logging.getLogger("requests").setLevel(logging.WARNING)

handler = logging.FileHandler('osrm_query_2kmb.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


def main(limit=5000, mode='walking', port=5000):
    '''
    Input:
        - Two shapefiles with coordinates and generates transit times between each pair.
    Output:
        - combined-data.db (SQL)
    '''
    # shapefiles
    orig_fn = '../data/por_block/por_blocks.shp' 
    dest_fn = orig_fn
    db_fn = '../query_results/por_5km.db'
    temp_fn = '../query_results/por-temp_5km.csv'
    db_temp_fn = '../query_results/por-temp_5km.db'

    # logger
    logger.info("Started with limit {} meters and mode {} on port {}".format(limit, mode, port))
    start = time.time()

    #Check for raw data
    CheckRawData(db_fn, orig_fn, dest_fn, temp_fn, db_temp_fn, mode)

    #Open connection to .db
    db = sqlite3.connect(db_fn) 
    cursor = db.cursor()

    #Get length of data to process
    BATCH_SIZE = 500
    cursor.execute('''SELECT COUNT(*) FROM origxdest'''.format(float(limit))) 
    data_len = cursor.fetchone()[0] / BATCH_SIZE
    logger.info('Queries to execute: {}'.format(data_len))

    #Query .db
    cursor.execute('''SELECT origxdest.orig_id, origxdest.dest_id, orig_lon, orig_lat, 
        dest_lon, dest_lat FROM origxdest
        INNER JOIN orig ON orig.orig_id = origxdest.orig_id
        INNER JOIN dest ON dest.dest_id = origxdest.dest_id
        WHERE euclidean < {}'''.format(limit))

    #Set multiprocessing
    no_cores = multiprocessing.cpu_count()
    if no_cores <= 10:
        no_cores -= 2
    else:
        no_cores -= 4

    #Form queue of workers
    queue = Queue()
    for x in range(no_cores):
        worker = QueryWorker(queue, port, mode, temp_fn)
        worker.daemon = True
        worker.start()

    #Calculate milestones for display
    percentages = {}
    for i in range(1, 20):
        actual = round(data_len * i / 20)
        percentages[actual] = round(i / 20 * 100)

    query_start = time.time()
    #Add workers to queue (multiprocessing)
    logger.info('Started querying OSRM server')
    data = cursor.fetchone()
    dests = []
    prev_orig_id = ''
    prev_orig_lon = ''
    prev_orig_lat = ''
    counter = 0
    while data:
        dests.append((data[1], data[4], data[5]))
        if (counter % BATCH_SIZE == 0 or prev_orig_id != data[0]) and counter != 0:
            pair = OrigxMany(data[0], data[2], data[3], dests, percentages)
            queue.put(pair)
            dests = []
        prev_orig_id = data[0]
        prev_orig_lon = data[2]
        prev_orig_lat = data[3]
        counter += 1
        data = cursor.fetchone()
    db.close()
    queue.join()
    query_end = time.time()
    logger.info('Done querying OSRM server ({} seconds)'.format(query_end - query_start))

    #transfer to .db
    database.Write(mode, temp_fn, db_fn)


    #show timer
    end = time.time()
    secs = round(end - start,2)
    mins = round(secs / 60, 2)
    hrs = round(secs / 3600,2)
    days = round(secs / (3600 * 24),2)
    end_string = 'Calculations took {} seconds, {} minutes, {} hours, or {} days'
    logger.info(end_string.format(secs, mins, hrs, days))
    logger.info("Data saved in combined-data.db")


    
def CheckRawData(db_fn, orig_fn, dest_fn, temp_fn, db_temp_fn, mode):
    '''
    check that data doesn't exist already
    if not, init database
    '''
    if not os.path.isfile(db_fn):
        database.Init(db_fn, orig_fn, dest_fn, db_temp_fn)
        euclidean.calculate(mode, db_fn, db_temp_fn)
    else:
        logger.info('Found combined-data.db')
    if os.path.isfile(temp_fn):
        os.remove(temp_fn)
        logger.info('Deleting old temp data')

    with open(temp_fn, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['0','1','2'])

class OrigxMany():
    '''
    Data structure containing the data for a single query to the OSRM server.
    '''
    id_ = 0
    def __init__(self, orig_id, orig_lon, orig_lat, dests, percentages):
        if OrigxMany.id_ in percentages:
            self.completion = percentages[OrigxMany.id_]
        else:
            self.completion = None
        self.orig_id = orig_id
        self.dests = dests
        self.orig_lon = orig_lon
        self.orig_lat = orig_lat
        OrigxMany.id_ += 1


class QueryWorker(Thread):
    '''
    A single thread, which executes querying tasks.
    '''
    def __init__(self, queue, port, mode, temp_fn):
       Thread.__init__(self)
       self.queue = queue
       self.port = port
       self.mode = mode
       self.qsize = self.queue.qsize()
       self.temp_fn = temp_fn

    def run(self):
        while True:
            pair = self.queue.get()
            QueryOSRM(pair, self.port, self.mode, self.temp_fn)
            self.queue.task_done()


def QueryOSRM(pair, port, mode, temp_fn):
    '''
    Sends a query to a local OSRM server. Expects a JSON as a response,
    which this function then parses and writes to file
    '''
    #Form and parse server request
    base_query = 'http://localhost:{}/table/v1/{}/{},{}'.format(port,mode,pair.orig_lon,pair.orig_lat)
    mid_query = ''
    end_query = '?sources=0'

    for i, dest_data in enumerate(pair.dests):
        dest_id, dest_lon, dest_lat = dest_data
        mid_query += ';' + str(dest_lon) + ',' + str(dest_lat)


    url = base_query + mid_query + end_query
    r = requests.get(url)

    res = r.json()['durations'][0][1:]

    with open(temp_fn, 'a', newline='') as f:
        writer = csv.writer(f)
        for i, dest_data in enumerate(pair.dests):
            dest_id, dest_lon, dest_lat = dest_data
            fields = [pair.orig_id, dest_id, int(res[i])]
  
            writer.writerow(fields)
    if pair.completion:
        logger.info("{} percent completed querying task".format(pair.completion))   

    



if __name__ == '__main__':
    logger.info("Running in main mode")
    main()
