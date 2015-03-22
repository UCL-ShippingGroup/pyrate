import logging
import time
import contextlib
import threading
import queue
from pyrate import utils

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

def generate_squeaky_table(aisdb):
    valid, intervals = filter_good_ships(aisdb)
    
    logging.info("Inserting %d squeaky clean ships", len(valid))

    with aisdb.conn.cursor() as cur:
        start = time.time()
        cur.execute("SELECT DISTINCT mmsi from {}".format(aisdb.squeaky.name))
        existing_mmsis = set([row[0] for row in cur.fetchall()])
        logging.info("%d MMSIs already imported (%fs)", len(existing_mmsis), time.time() - start)

    mmsi_list = set([row[0] for row in intervals]) - existing_mmsis
    total = len(mmsi_list)
    logging.info("%d MMSIs to insert", total)        
        
    
    mmsi_q = queue.Queue()
    for mmsi in sorted(mmsi_list):
        mmsi_q.put(mmsi)

    pool = [threading.Thread(target=mmsi_copier, daemon=True, args=(aisdb, mmsi_q)) for i in range(10)]
    [t.start() for t in pool]
    
    remain = total
    start = time.time()
    while(not mmsi_q.empty()):
        q_size = mmsi_q.qsize()
        if remain > q_size:
            logging.info("%d/%d MMSIs completed, %f/s.", total - q_size, total, (total - q_size) / (time.time() - start))
            remain = q_size
        time.sleep(5)
    mmsi_q.join()

def mmsi_copier(aisdb, mmsi_q):
    logging.debug("Start MMSI copier task")
    with contextlib.closing(aisdb.connection()) as conn:
        while not mmsi_q.empty():
            mmsi = mmsi_q.get()
            with aisdb.conn.cursor() as cur:
                start = time.time()
                cols_list = ','.join([c[0].lower() for c in aisdb.squeaky.cols])
                select_sql = "SELECT {} FROM {} WHERE mmsi = %s".format(cols_list, aisdb.clean.name)
                cur.execute("INSERT INTO {} {}".format(aisdb.squeaky.name, select_sql), [mmsi])
                logging.debug("Inserted %d rows for MMSI %d. (%fs)", cur.rowcount, mmsi, time.time() - start)
            conn.commit()
            mmsi_q.task_done()

def _mmsi_copy(aisdb, mmsi):
    with contextlib.closing(aisdb.connection()) as conn:
        with aisdb.conn.cursor() as cur:
            start = time.time()
            cols_list = ','.join([c[0].lower() for c in aisdb.squeaky.cols])
            select_sql = "SELECT {} FROM {} WHERE mmsi = %s".format(cols_list, aisdb.clean.name)
            cur.execute("INSERT INTO {} {}".format(aisdb.squeaky.name, select_sql), [mmsi])
            logging.debug("Inserted %d rows for MMSI %d. (%fs)", cur.rowcount, mmsi, time.time() - start)
        conn.commit()
    

def _copy_table_mmsi(aisdb, mmsi):
    with aisdb.conn.cursor() as cur:
        cols_list = ','.join([c[0].lower() for c in aisdb.squeaky.cols])
        select_sql = "SELECT {} FROM {} WHERE mmsi = %s".format(cols_list, aisdb.clean.name)
        cur.execute("INSERT INTO {} {}".format(aisdb.squeaky.name, select_sql), [mmsi])
        return cur.rowcount

def _copy_table_subset_mmsis(aisdb, mmsis):
    if len(mmsis) == 0:
        return

    with aisdb.conn.cursor() as cur:
        cols_list = ','.join([c[0].lower() for c in aisdb.squeaky.cols])
        mmsi_list = ','.join(['%s'] * len(mmsis))
        cur.execute("INSERT INTO {} SELECT {} FROM {} WHERE mmsi IN ({})".format(aisdb.squeaky.name, cols_list, aisdb.clean.name, mmsi_list), mmsis)
        return cur.rowcount
