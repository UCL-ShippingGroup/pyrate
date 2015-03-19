import logging
import time
from pyrate import utils

EXPORT_COMMANDS = [('run', 'create or update the imo list table.')]
INPUTS = []
OUTPUTS = ["aisdb"]

def run(inp, out):
    create_imo_list(out['aisdb'])

def create_imo_list(aisdb):
    """Create the imo list table from mmsi, imo pairs in clean and dirty tables.

    This method collects the unique mmsi, imo pairs from a table, and the time
    intervals over-which they have been seen in the data. These tuples are
    then upserted into the imo_list table.

    On the clean table pairs with no IMO number are also collected to get the
    activity intervals of MMSI numbers. On the dirty table only messages
    specifying an IMO are collected."""

    with aisdb.conn.cursor() as cur:
        start = time.time()

        # collect existing set of mmsi, imo tuples in imo_list
        cur.execute("SELECT mmsi, imo FROM {}".format(aisdb.imolist.get_name()))
        existing_tuples = set(cur.fetchall())
        logging.info("Existing mmsi, imo pairs = %d (%fs)", len(existing_tuples), time.time()-start)

        # query for mmsi, imo, interval tuples from clean db, and then upsert into
        # imo_list table.
        logging.info("Getting mmsi, imo pairs from clean db")
        start = time.time()
        cur.execute("SELECT mmsi, imo, MIN(time), MAX(time) FROM {} GROUP BY mmsi, imo".format(aisdb.clean.get_name()))
        logging.info("Got new mmsi, imo pairs list (%fs)", time.time()-start)
        _upsert_imo_tuples(aisdb, cur, existing_tuples)

        # query for mmsi, imo, interval tuples from dirty db, and then upsert into
        # imo_list table.
        logging.info("Getting mmsi, imo pairs from dirty db")
        start = time.time()
        cur.execute("SELECT mmsi, imo, MIN(time), MAX(time) FROM {} WHERE message_id = 5 GROUP BY mmsi, imo".format(aisdb.dirty.get_name()))
        logging.info("Got new mmsi, imo pairs list (%fs)", time.time()-start)
        _upsert_imo_tuples(aisdb, cur, existing_tuples)

        aisdb.conn.commit()

def _upsert_imo_tuples(aisdb, result_cursor, existing_tuples):
    """Inserts or updates rows in the imo_list table depending on the mmsi, imo
    pair's presence in the table. result_cursor is a iterator of (mmsi, imo,
    start, end) tuples. existing_tuples is a set of (mmsi, imo) pairs which
    should be updated rather than inserted.
    """

    with aisdb.conn.cursor() as insert_cur:
        start = time.time()
        insert_ctr = 0
        update_ctr = 0
        for mmsi, imo, min_time, max_time in result_cursor:
            if (mmsi, imo) in existing_tuples:
                insert_cur.execute("""UPDATE {} SET
                    first_seen = LEAST(first_seen, %s), 
                    last_seen = GREATEST(last_seen, %s)
                    WHERE mmsi = %s AND imo = %s""".format(aisdb.imolist.get_name()), [min_time, max_time, mmsi, imo])
                update_ctr = update_ctr + insert_cur.rowcount
            else:
                insert_cur.execute("INSERT INTO {} (mmsi, imo, first_seen, last_seen) VALUES (%s,%s,%s,%s)".format(aisdb.imolist.get_name()), [mmsi, imo, min_time, max_time])
                insert_ctr = insert_ctr + 1
        logging.info("Inserted %d new rows, updated %d rows (%f)", insert_ctr, update_ctr, time.time()-start)

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

    i = 0
    total = len(intervals) - len(existing_mmsis)
    mmsi_list = [row[0] for row in intervals]
    
    for mmsi in sorted(mmsi_list):
        if mmsi in existing_mmsis:
            continue
        else:
            i = i + 1
            start = time.time()
            count = _copy_table_mmsi(aisdb, mmsi)
            logging.debug("Inserted %d rows for MMSI %d. (%d/%d) (%fs)", count, mmsi, i, total, time.time() - start)
            aisdb.conn.commit()

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
