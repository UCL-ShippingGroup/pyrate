import logging
import time

EXPORT_COMMANDS = [('run', 'create or update the imo list table.')]
INPUTS = []
OUTPUTS = ["aisdb"]

def run(_, out):
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
