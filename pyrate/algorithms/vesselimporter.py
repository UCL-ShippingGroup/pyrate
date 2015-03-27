import logging
import time
import threading
import psycopg2
import queue
from pyrate import utils

EXPORT_COMMANDS = [('run', 'Extract a subset of clean ships into ais_extended tables')]
INPUTS = []
OUTPUTS = ['aisdb']

def run(inp, out, n_threads=2, dropindices=False):
    aisdb = out['aisdb']
    valid_imos, imo_mmsi_intervals = filter_good_ships(aisdb)
    logging.info("Got %d valid IMO numbers, using %d MMSI numbers", len(valid_imos), len(imo_mmsi_intervals))
    # pre filter intervals
    def filter_intervals(interval):
        remain = get_remaining_interval(aisdb, *interval)
        if not remain is None:
            return [interval[0], interval[1], remain[0], remain[1]]
        else:
            return None
    filtered_intervals = map(filter_intervals, imo_mmsi_intervals)
    # sorting intervals by MMSI improves performance on a clustered table
    sorted_intervals = sorted(filter(None, filtered_intervals), key=lambda x: x[0])
    logging.info("%d intervals to import", len(sorted_intervals))
    # insert ships into extended table.
    if len(sorted_intervals) > 0:
        if dropindices:
            aisdb.extended.drop_indices()
        generate_extended_table(aisdb, sorted_intervals, n_threads=n_threads)
        if dropindices:
            aisdb.extended.create_indices()
    logging.info("Vessel importer done.")

def filter_good_ships(aisdb):
    """Generate a set of imo numbers and (mmsi, imo) validity intervals, for
    ships which are deemed to be 'clean'. A clean ship is defined as one which:
     * Has valid MMSI numbers associated with it.
     * For each MMSI number, the period of time it is associated with this IMO
    (via message number 5) overlaps with the period the MMSI number was in use.
     * For each MMSI number, its usage period does not overlap with that of any
    other of this ship's MMSI numbers.
     * That none of these MMSI numbers have been used by another ship (i.e.
    another IMO number is also associated with this MMSI)

    Returns the tuple (valid_imos, imo_mmsi_intervals), where:
     * valid_imos is a set of valid imo numbers
     * imo_mmsi_intervals is a list of (mmsi, imo, start, end) tuples, describing
     the validity intervals of each mmsi, imo pair.
    """

    with aisdb.conn.cursor() as cur:
        cur.execute("SELECT distinct imo from {}".format(aisdb.imolist.get_name()))
        imo_list = [row[0] for row in cur.fetchall() if utils.valid_imo(row[0])]
        logging.info("Checking %d IMOs", len(imo_list))
        
        valid_imos = []
        imo_mmsi_intervals = []

        for imo in imo_list:
            cur.execute("""select a.mmsi, a.imo, (a.first_seen, a.last_seen) overlaps (b.first_seen, b.last_seen), LEAST(a.first_seen, b.first_seen), GREATEST(a.last_seen, b.last_seen)
                    from imo_list as a
                    join imo_list as b on a.mmsi = b.mmsi and b.imo is null
                    where a.imo = %s
                    ORDER BY LEAST(a.first_seen, b.first_seen) ASC""", [imo])
            mmsi_ranges = cur.fetchall()
            if len(mmsi_ranges) == 0:
                #logging.info("No MMSI numbers for IMO %s", imo)
                continue

            valid = True
            last_end = None
            for mmsi, _, overlap, start, end in mmsi_ranges:
                if not overlap:
                    valid = False
                    #logging.info("(%s, %s) does not overlap (%s, _)", mmsi, imo, mmsi)
                    break
                if last_end != None and start < last_end:
                    valid = False
                    #logging.info("IMO: %s, overlapping MMSI intervals", imo)
                    break;
                last_end = end
            
            if valid:
                # check for other users of this mmsi number
                mmsi_list = [row[0] for row in mmsi_ranges]
                cur.execute("""select a.mmsi, a.imo, b.imo
                    from imo_list as a
                    join imo_list as b on a.mmsi = b.mmsi and a.imo < b.imo
                    where a.mmsi IN ({})""".format(','.join(['%s' for i in mmsi_list])), mmsi_list)
                if cur.rowcount == 0:
                    # yay its valid!
                    valid_imos.append(imo)
                    for mmsi, _, _, start, end in mmsi_ranges:
                        imo_mmsi_intervals.append([mmsi, imo, start, end])
                else:
                    pass
                    #logging.info("IMO: %s, reuse of MMSI", imo)
    
        return (valid_imos, imo_mmsi_intervals)

def cluster_table(aisdb, table):
    """Performs a clustering of the postgresql table on the MMSI index. This
    process significantly improves the runtime of extended table generation."""
    with aisdb.conn.cursor() as cur:
        index_name = table.name.lower() + "_mmsi_idx"
        logging.info("Clustering table %s on index %s. This may take a while...",
                     table.name, index_name)
        cur.execute("CLUSTER {} USING {}".format(table.name, index_name))

def generate_extended_table(aisdb, intervals, n_threads=2):

    logging.info("Inserting %d squeaky clean MMSIs", len(intervals))     
    
    start = time.time()

    interval_q = queue.Queue()
    for interval in sorted(intervals, key=lambda x: x[0]):
        interval_q.put(interval)

    pool = [threading.Thread(target=interval_copier, daemon=True, args=(aisdb.options, interval_q)) for i in range(n_threads)]
    [t.start() for t in pool]

    total = len(intervals)
    remain = total
    start = time.time()
    while not interval_q.empty():
        q_size = interval_q.qsize()
        if remain > q_size:
            logging.info("%d/%d MMSIs completed, %f/s.", total - q_size, total, (total - q_size) / (time.time() - start))
            remain = q_size
        time.sleep(5)
    interval_q.join()

def interval_copier(db_options, interval_q):
    from pyrate.repositories import aisdb as db
    aisdb = db.load(db_options)
    logging.debug("Start interval copier task")
    with aisdb:
        while not interval_q.empty():
            interval = interval_q.get()
            process_interval_series(aisdb, interval)
            interval_q.task_done()

def process_interval_series(aisdb, interval):
    mmsi, imo, start, end = interval
    t_start = time.time()
    # constrain interval based on previous import
    remaining_work = get_remaining_interval(aisdb, mmsi, imo, start, end)
    if remaining_work is None:
        #logging.info("Interval was already inserted: (%s, %s, %s, %s)", mmsi, imo, start, end)
        return 0
    else:
        start, end = remaining_work

    # get data for this interval range
    msg_stream = aisdb.get_message_stream(mmsi, from_ts=start, to_ts=end, use_clean_db=True)
    row_count = len(msg_stream)
    if row_count == 0:
        logging.warning("No rows to insert for interval %s", interval)
        return 0

    insert_message_stream(aisdb, [mmsi, imo, start, end], msg_stream)

    # finished, commit
    aisdb.conn.commit()
    logging.debug("Inserted %d rows for MMSI %d. (%fs)", row_count, mmsi, time.time() - t_start)
    return row_count

def insert_message_stream(aisdb, interval, msg_stream):
    """Takes a stream of messages for an MMSI over an interval, runs it through
    outlier detection and interpolation algorithms, then inserts the resulting
    stream into the ais_extended table."""
    mmsi, imo, start, end = interval
    # call the message filter
    valid, invalid = utils.detect_outliers(msg_stream)
    artificial = utils.interpolate_passages(valid)

    aisdb.extended.insert_rows_batch(valid + artificial)

    # mark the work we've done
    aisdb.action_log.insert_row({'action': "import", 
                                 'mmsi': mmsi,
                                 'ts_from': start, 
                                 'ts_to': end,
                                 'count': len(valid)})
    aisdb.action_log.insert_row({'action': "outlier detection (noop)", 
                                 'mmsi': mmsi, 
                                 'ts_from': start, 
                                 'ts_to': end,
                                 'count': len(invalid)})
    aisdb.action_log.insert_row({'action': "interpolation (noop)", 
                                 'mmsi': mmsi, 
                                 'ts_from': start, 
                                 'ts_to': end,
                                 'count': len(artificial)})
    upsert_interval_to_imolist(aisdb, mmsi, imo, start, end)

def get_remaining_interval(aisdb, mmsi, imo, start, end):
    with aisdb.conn.cursor() as cur:
        try:
            cur.execute("SELECT tsrange(%s, %s) - tsrange(%s, %s) * tsrange(first_seen - interval '1 second', last_seen + interval '1 second') FROM {} WHERE mmsi = %s AND imo = %s".format(aisdb.clean_imolist.name), 
                        [start, end, start, end, mmsi, imo])
            row = cur.fetchone()
            if not row is None:
                sub_interval = row[0]
                if sub_interval.isempty:
                    return None
                else:   
                    return sub_interval.lower, sub_interval.upper
            else:
                return (start, end)
        except psycopg2.Error as e:
            logging.warning("Error calculating timetamp intersection for MMSI %d: %s", mmsi, e)
            return None

def upsert_interval_to_imolist(aisdb, mmsi, imo, start, end):
    with aisdb.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM {} WHERE mmsi = %s AND imo = %s"
                     .format(aisdb.clean_imolist.name),
                     [mmsi, imo])
        count = cur.fetchone()[0]
        if count == 1:
            cur.execute("""UPDATE {} SET
                        first_seen = LEAST(first_seen, %s), 
                        last_seen = GREATEST(last_seen, %s)
                        WHERE mmsi = %s AND imo = %s""".format(aisdb.clean_imolist.name),
                        [start, end, mmsi, imo])
        elif count == 0:
            aisdb.clean_imolist.insert_row({'mmsi': mmsi, 'imo': imo, 'first_seen': start, 
                                          'last_seen': end})
