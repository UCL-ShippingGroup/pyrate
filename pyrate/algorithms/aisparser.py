""" Parses the AIS data from csv of xml files and populates the AIS database
"""

import os
import csv
import logging
import queue
import threading
import time
import sys
from datetime import datetime
from xml.etree import ElementTree
from pyrate import utils

EXPORT_COMMANDS = [('run', 'parse messages from csv into the database.')]
# Repository used for input to the algorithm
INPUTS = ["aiscsv"]
# Repositories used for output from the algorithm
OUTPUTS = ["aisdb", "baddata"]

def parse_timestamp(s):
    return datetime.strptime(s, '%Y%m%d_%H%M%S')

def int_or_null(s):
    if len(s) == 0:
        return None
    else:
        return int(s)

def float_or_null(s):
    if len(s) == 0 or s == 'None':
        return None
    else:
        return float(s)

def imostr(s):
    if len(s) > 20:
        return None
    return s

def longstr(s):
    if len(s) > 255:
        return s.substring(0, 254)
    return s

def set_null_on_fail(row, col, test):
    """ Helper function which sets the column in a row of data to null on fail

    Arguments
    ---------
    row : dict
        A dictionary of the fields
    col : str
        The column to check
    test : func
        One of the validation functions in pyrate.utils
    """
    if not row[col] == None and not test(row[col]):
        row[col] = None

def check_imo(imo):
    return imo is None or utils.valid_imo(imo)

# column name constants
MMSI = 'MMSI'
TIME = 'Time'
MESSAGE_ID = 'Message_ID'
NAV_STATUS = 'Navigational_status'
SOG = 'SOG'
LONGITUDE = 'Longitude'
LATITUDE = 'Latitude'
COG = 'COG'
HEADING = 'Heading'
IMO = 'IMO'
DRAUGHT = 'Draught'
DEST = 'Destination'
VESSEL_NAME = 'Vessel_Name'
ETA_MONTH = 'ETA_month'
ETA_DAY = 'ETA_day'
ETA_HOUR = 'ETA_hour'
ETA_MINUTE = 'ETA_minute'

# specifies columns to take from raw data, and functions to convert them into
# suitable type for the database.
AIS_CSV_COLUMNS = [MMSI,
                   TIME,
                   MESSAGE_ID,
                   NAV_STATUS,
                   SOG,
                   LONGITUDE,
                   LATITUDE,
                   COG,
                   HEADING,
                   IMO,
                   DRAUGHT,
                   DEST,
                   VESSEL_NAME,
                   ETA_MONTH,
                   ETA_DAY,
                   ETA_HOUR,
                   ETA_MINUTE]

# xml names differ from csv. This array describes the names in this file which
# correspond to the csv column names
AIS_XML_COLNAMES = [
    'mmsi',
    'date_time',
    'msg_type',
    'nav_status',
    'sog',
    'lon',
    'lat',
    'cog',
    'heading',
    'imo',
    'draught',
    'destination',
    'vessel_name',
    'eta_month',
    'eta_day',
    'eta_hour',
    'eta_minute']

def xml_name_to_csv(name):
    """Converts a tag name from an XML file to the corresponding name from CSV."""
    return AIS_CSV_COLUMNS[AIS_XML_COLNAMES.index(name)]

def parse_raw_row(row):
    """Parse values from row, returning a new dict with converted values

    Parse values from row, returning a new dict with converted values
    converted into appropriate types. Throw an exception to reject row

    Arguments
    ---------
    row : dict
        A dictionary of headers and values from the csv file

    Returns
    -------
    converted_row : dict
        A dictionary of headers and values converted using the helper functions

    """
    converted_row = {}
    converted_row[MMSI] = int_or_null(row[MMSI])
    converted_row[TIME] = parse_timestamp(row[TIME])
    converted_row[MESSAGE_ID] = int_or_null(row[MESSAGE_ID])
    converted_row[NAV_STATUS] = int_or_null(row[NAV_STATUS])
    converted_row[SOG] = float_or_null(row[SOG])
    converted_row[LONGITUDE] = float_or_null(row[LONGITUDE])
    converted_row[LATITUDE] = float_or_null(row[LATITUDE])
    converted_row[COG] = float_or_null(row[COG])
    converted_row[HEADING] = float_or_null(row[HEADING])
    converted_row[IMO] = int_or_null(row[IMO])
    converted_row[DRAUGHT] = float_or_null(row[DRAUGHT])
    converted_row[DEST] = longstr(row[DEST])
    converted_row[VESSEL_NAME] = longstr(row[VESSEL_NAME])
    converted_row[ETA_MONTH] = int_or_null(row[ETA_MONTH])
    converted_row[ETA_DAY] = int_or_null(row[ETA_DAY])
    converted_row[ETA_HOUR] = int_or_null(row[ETA_HOUR])
    converted_row[ETA_MINUTE] = int_or_null(row[ETA_MINUTE])
    return converted_row

CONTAINS_LAT_LON = set([1, 2, 3, 4, 9, 11, 17, 18, 19, 21, 27])

def validate_row(row):
    # validate MMSI, message_id and IMO
    if not utils.valid_mmsi(row[MMSI]) \
       or not utils.valid_message_id(row[MESSAGE_ID]) \
       or not check_imo(row[IMO]):
        raise ValueError("Row invalid")
    # check lat long for messages which should contain it
    if row[MESSAGE_ID] in CONTAINS_LAT_LON:
        if not (utils.valid_longitude(row[LONGITUDE]) and \
           utils.valid_latitude(row[LATITUDE])):
            raise ValueError("Row invalid (lat,lon)")
    # otherwise set them to None
    else:
        row[LONGITUDE] = None
        row[LATITUDE] = None

    # validate other columns
    set_null_on_fail(row, NAV_STATUS, utils.valid_navigational_status)
    set_null_on_fail(row, SOG, utils.is_valid_sog)
    set_null_on_fail(row, COG, utils.is_valid_cog)
    set_null_on_fail(row, HEADING, utils.is_valid_heading)
    return row

def get_data_source(name):
    """Guesses data source from file name.

    If the name contains 'terr' then we guess terrestrial data,
    otherwise we assume satellite.

    Arguments
    =========
    name : str
        File name

    Returns
    =======
    int
        0 if satellite, 1 if terrestrial

    """
    if name.find('terr') != -1:
        # terrestrial
        return 1
    else:
        # assume satellite
        return 0

def run(inp, out, dropindices=True, source=0):
    """Populate the AIS_Raw database with messages from the AIS csv files

    Arguments
    ---------
    inp : str
        The name of the repositor(-y/-ies) as defined in the global variable
        `INPUTS`
    out : str
        The name of the repositor(-y/-ies) as defined in the global variable
        `OUTPUTS`
    dropindices : bool, optional, default=True
        Drop indexes for faster insert
    source : int, optional, default=0
        Indicates terrestrial (1) or satellite data (0)

    Returns
    -------
    """

    files = inp['aiscsv']
    db = out['aisdb']
    log = out['baddata']

    # drop indexes for faster insert
    if dropindices:
        db.clean.drop_indices()
        db.dirty.drop_indices()

    def sqlworker(q, table):
        """ Worker thread

        Takes batches of tuples from the queue to be inserted into the database
        """
        while True:
            msgs = [q.get()]
            while not q.empty():
                msgs.append(q.get(timeout=0.5))

            n = len(msgs)
            if n > 0:
                #logging.debug("Inserting {} rows into {}".format(n, table.name))
                try:
                    table.insert_rows_batch(msgs)
                except Exception as e:
                    logging.warning("Error executing query: "+ repr(e))
            # mark this task as done
            for _ in range(n):
                q.task_done()

    # queue for messages to be inserted into db
    dirtyq = queue.Queue(maxsize=1000000)
    cleanq = queue.Queue(maxsize=1000000)
    # set up processing pipeline threads
    clean_thread = threading.Thread(target=sqlworker, daemon=True,
                                    args=(cleanq, db.clean))
    dirty_thread = threading.Thread(target=sqlworker, daemon=True,
                                    args=(dirtyq, db.dirty))
    dirty_thread.start()
    clean_thread.start()

    start = time.time()

    for fp, name, ext in files.iterfiles():
        # check if we've already parsed this file
        with db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM " + db.sources.name +
                        " WHERE filename = %s AND source = %s",
                        [name, source])
            if cur.fetchone()[0] > 0:
                logging.info("Already parsed "+ name +", skipping...")
                continue

        # parse file
        try:
            log_path = os.path.join(log.root, os.path.basename(name))
            invalid_ctr, clean_ctr, dirty_ctr, duration = parse_file(fp,
                            name, ext, log_path, cleanq, dirtyq, source=source)
            dirtyq.join()
            cleanq.join()
            db.sources.insert_row({'filename': name,
                                   'ext': ext,
                                   'invalid': invalid_ctr,
                                   'clean': clean_ctr,
                                   'dirty': dirty_ctr,
                                   'source': source})
            db.conn.commit()
            logging.info("Completed " + name +
                         ": %d clean, %d dirty, %d invalid messages, %fs",
                         clean_ctr, dirty_ctr, invalid_ctr, duration)
        except RuntimeError as error:
            logging.warn("Error parsing file %s: %s", name, repr(error))
            db.conn.rollback()

    # wait for queued tasks to finish
    dirtyq.join()
    cleanq.join()
    db.conn.commit()

    logging.info("Parsing complete, time elapsed = %fs", time.time() - start)

    if dropindices:
        start = time.time()
        logging.info("Rebuilding table indices...")
        db.clean.create_indices()
        db.dirty.create_indices()
        logging.info("Finished building indices, time elapsed = %fs",
                     time.time() - start)


def parse_file(fp, name, ext, baddata_logfile, cleanq, dirtyq, source=0):
    """ Parses a file containing AIS data, placing rows of data onto queues

    Arguments
    ---------
    fp : str
        Filepath of file to be parsed
    name : str
        Name of file to be parsed
    ext : str
        Extension, either '.csv' or '.xml'
    baddata_logfile : str
        Name of the logfile
    cleanq :
        Queue for messages to be inserted into clean table
    dirtyq :
        Queue for messages to be inserted into dirty table
    source : int, optional, default=0
        0 is satellite, 1 is terrestrial

    Returns
    -------
    invalid_ctr : int
        Number of invalid rows
    clean_ctr : int
        Number of clean rows
    dirty_ctr : int
        Number of dirty rows
    time_elapsed : time
        The time elapsed since starting the parse_file procedure
    """
    filestart = time.time()
    logging.info("Parsing "+ name)

    # open error log csv file and write header
    with open(baddata_logfile, 'w') as errorlog:
        logwriter = csv.writer(errorlog, delimiter=',', quotechar='"')

        # message counters
        clean_ctr = 0
        dirty_ctr = 0
        invalid_ctr = 0

        # Select the a file iterator based on file extension
        if ext == '.csv':
            iterator = readcsv
        elif ext == '.xml':
            iterator = readxml
        else:
            raise RuntimeError("Cannot parse file with extension %s"% ext)

        # infer the data source from the file name
        #source = get_data_source(name)

        # parse and iterate lines from the current file
        for row in iterator(fp):
            converted_row = {}
            try:
                # parse raw data
                converted_row = parse_raw_row(row)
                converted_row['source'] = source
            except ValueError as e:
                # invalid data in row. Write it to error log
                if not 'raw' in row:
                    row['raw'] = [row[c] for c in AIS_CSV_COLUMNS]
                logwriter.writerow(row['raw'] + ["{}".format(e)])
                invalid_ctr = invalid_ctr + 1
                continue
            except KeyError:
                # missing data in row.
                if not 'raw' in row:
                    row['raw'] = [row[c] for c in AIS_CSV_COLUMNS]
                logwriter.writerow(row['raw'] + ["Bad row length"])
                invalid_ctr = invalid_ctr + 1
                continue

            # validate parsed row and add to appropriate queue
            try:
                validated_row = validate_row(converted_row)
                cleanq.put(validated_row)
                clean_ctr = clean_ctr + 1
            except ValueError:
                dirtyq.put(converted_row)
                dirty_ctr = dirty_ctr + 1

    if invalid_ctr == 0:
        os.remove(baddata_logfile)

    return (invalid_ctr, clean_ctr, dirty_ctr, time.time() - filestart)


def readcsv(fp):
    """ Returns a dictionary of the subset of columns required

    Reads each line in CSV file, checks if all columns are available,
    and returns a dictionary of the subset of columns required
    (as per AIS_CSV_COLUMNS).

    If row is invalid (too few columns),
    returns an empty dictionary.

    Arguments
    ---------
    fp : str
        File path

    Yields
    ------
    rowsubset : dict
        A dictionary of the subset of columns as per `columns`

    """
    # fix for large field error. Specify max field size to the maximum convertable int value.
    # source: http://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
    max_int = sys.maxsize
    decrement = True
    while decrement:
        # decrease the max_int value by factor 10
        # as long as the OverflowError occurs.
        decrement = False
        try:
            csv.field_size_limit(max_int)
        except OverflowError:
            max_int = int(max_int/10)
            decrement = True

    # First line is column headers.
    # Use to extract indices of columns we are extracting
    cols = fp.readline().rstrip('\r\n').split(',')
    indices = {}
    n_cols = len(cols)
    try:
        for col in AIS_CSV_COLUMNS:
            indices[col] = cols.index(col)
    except Exception as e:
        raise RuntimeError("Missing columns in file header: {}".format(e))

    try:
        for row in csv.reader(fp, delimiter=',', quotechar='"'):
            rowsubset = {}
            rowsubset['raw'] = row
            if len(row) == n_cols:
                for col in AIS_CSV_COLUMNS:
                    try:
                        rowsubset[col] = row[indices[col]] # raw column data
                    except IndexError:
                        # not enough columns, just blank missing data.
                        rowsubset[col] = ''
            yield rowsubset
    except UnicodeDecodeError as e:
        raise RuntimeError(e)
    except csv.Error as e:
        raise RuntimeError(e)

def readxml(fp):
    current = _empty_row()
    # iterate xml 'end' events
    for _, elem in ElementTree.iterparse(fp):
        # end of aismessage
        if elem.tag == 'aismessage':
            yield current
            current = _empty_row()
        else:
            if elem.tag in AIS_XML_COLNAMES and elem.text != None:
                current[xml_name_to_csv(elem.tag)] = elem.text

def _empty_row():
    row = {}
    for col in AIS_CSV_COLUMNS:
        row[col] = ''
    return row
