import logging
import time

EXPORT_COMMANDS = [('run', 'create or update the imo list table.')]
INPUTS = []
OUTPUTS = ["aisdb"]

def run(inp, out):
    create_imo_list(out['aisdb'])

def create_imo_list(aisdb):

    # insert data from clean db
    with aisdb.conn.cursor() as cur, aisdb.conn.cursor() as insert_cur:
        start = time.time()
        cur.execute("SELECT mmsi, imo FROM {}".format(aisdb.imolist.get_name()))
        existing_tuples = set(cur.fetchall())
        logging.info("Existing mmsi, imo pairs = %d (%fs)", len(existing_tuples), time.time()-start)

        logging.info("Getting mmsi, imo pairs from clean db")
        # message_id = 5: 800s
        start = time.time()
        cur.execute("SELECT mmsi, imo, MIN(time), MAX(time) FROM {} GROUP BY mmsi, imo".format(aisdb.clean.get_name()))
        logging.info("Got new mmsi, imo pairs list (%fs)", time.time()-start)

        start = time.time()
        logging.info("Updating imo_list table")
        insert_ctr = 0
        update_ctr = 0
        for mmsi, imo, min_time, max_time in cur:
            if (mmsi, imo) in existing_tuples:
                insert_cur.execute("""UPDATE {} SET
                    first_seen = LEAST(first_seen, %s), 
                    last_seen = GREATEST(last_seen, %s)
                    WHERE mmsi = %s AND imo = %s""".format(aisdb.imolist.get_name()), [min_time, max_time, mmsi, imo])
                update_ctr = update_ctr + insert_cur.rowcount
            else:
                insert_cur.execute("INSERT INTO {} (mmsi, imo, first_seen, last_seen) VALUES (%s,%s,%s,%s)".format(aisdb.imolist.get_name()), [mmsi, imo, min_time, max_time])
                insert_ctr = insert_ctr + 1

        aisdb.conn.commit()
        logging.info("Inserted %d new rows, updated %d rows (%f)", insert_ctr, update_ctr, time.time()-start)
