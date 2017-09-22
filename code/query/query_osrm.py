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

handler = logging.FileHandler('osrm_query_2km.log')
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)


def main(limit=2000, mode='walking', port=5000):
    '''
    Input:
        - Two shapefiles with coordinates and generates transit times between each pair.
    Output:
        - combined-data.db (SQL)
    '''
    # shapefiles
    orig_fn = '../data/block_data/sea_blocks_wgs84.shp' 
    dest_fn = '../data/block_data/sea_blocks_wgs84.shp' 
    db_fn = '../query_results/combined-data_5km.db'
    temp_fn = '../query_results/temp-data_5km.csv'
    db_temp_fn = '../query_results/temp-data_5km.db'

    # logger
    logger.info("Started with limit {} meters and mode {} on port {}".format(limit, mode, port))
    start = time.time()

    #Check for raw data
    CheckRawData(db_fn, orig_fn, dest_fn, temp_fn, db_temp_fn, mode)

    #Open connection to .db
    db = sqlite3.connect(db_fn) 
    cursor = db.cursor()

    #Get length of data to process
    cursor.execute('''SELECT COUNT(*) FROM origxdest 
        WHERE euclidean < {}'''.format(float(limit)))
    data_len = cursor.fetchone()[0]

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
        no_cores -= 6

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

    #Add workers to queue (multiprocessing)
    logger.info('Started querying OSRM server')
    data = cursor.fetchone()

    while data:
        pair = OrigDestPair(data[0], data[1], data[2], data[3], data[4], 
                                data[5], percentages)
        queue.put(pair)
        data = cursor.fetchone()
    queue.join()
    logger.info('Done querying OSRM server')

    #read to .db and clean up
    logger.info('Transfering data to .db')
    with open(temp_fn) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            database.Write(row[0], row[1], row[2], mode, db_fn)

    #clean up
    #if os.path.isfile('temp-data.csv'):
    #    os.remove('temp-data.csv')
    #    logger.info('Deleting temp-data.csv')

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

class OrigDestPair():
    '''
    Data structure containing the data for a single query to the OSRM server.
    '''
    id_ = 0
    def __init__(self, orig_id, dest_id, orig_lon, orig_lat, dest_lon,
                 dest_lat, percentages):
        if OrigDestPair.id_ in percentages:
            self.completion = percentages[OrigDestPair.id_]
        else:
            self.completion = None
        self.orig_id = orig_id
        self.dest_id = dest_id
        self.orig_lon = orig_lon
        self.orig_lat = orig_lat
        self.dest_lon = dest_lon
        self.dest_lat = dest_lat
        OrigDestPair.id_ += 1


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
    url = 'http://ioe-picturedrocks.engin.umich.edu:{}/route/v1/{}/{},{};{},{}?overview=false'.format(port,mode,pair.orig_lon,pair.orig_lat,pair.dest_lon,pair.dest_lat)
    # url = 'http://localhost:{}/route/v1/{}/{},{};{},{}?overview=false'.format(port,mode,pair.orig_lon,pair.orig_lat,pair.dest_lon,pair.dest_lat)
    r = requests.get(url)
    duration = round(float(r.json()['routes'][0]['duration']),2)

    #Initialize connection to .db and update data
    #db = sqlite3.connect('combined-data.db',timeout=60)
    #cursor = db.cursor()
    #insert_str = '''UPDATE origxdest SET {}_time = {}
    #WHERE orig_id IS \'{}\' AND dest_id IS \'{}\''''.format(mode, duration, pair.orig_id, pair.dest_id)
    #cursor.execute(insert_str)
    #db.commit()

    #Close connection to .db
    #db.close()
    with open(temp_fn, 'a', newline='') as f:
        writer = csv.writer(f)
        fields = [pair.orig_id, pair.dest_id, duration]
        writer.writerow(fields)
    if pair.completion:
        logger.info("{} percent completed querying task".format(pair.completion))   

    return
    



if __name__ == '__main__':
    logger.info("Running in main mode")
    main()
