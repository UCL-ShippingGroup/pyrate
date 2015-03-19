import logging
import time
from pyrate import utils

EXPORT_COMMANDS = [('run', 'create or update the imo list table.')]
INPUTS = []
OUTPUTS = ["aisdb"]

def run(inp, out):
    create_imo_list(out['aisdb'])

def create_imo_list(aisdb):

    # insert data from clean db
    with aisdb.conn.cursor() as cur:
        start = time.time()
        cur.execute("SELECT mmsi, imo FROM {}".format(aisdb.imolist.get_name()))
        existing_tuples = set(cur.fetchall())
        logging.info("Existing mmsi, imo pairs = %d (%fs)", len(existing_tuples), time.time()-start)

        logging.info("Getting mmsi, imo pairs from clean db")
        start = time.time()
        cur.execute("SELECT mmsi, imo, MIN(time), MAX(time) FROM {} GROUP BY mmsi, imo".format(aisdb.clean.get_name()))
        logging.info("Got new mmsi, imo pairs list (%fs)", time.time()-start)
        _upsert_imo_tuples(aisdb, cur, existing_tuples)

        logging.info("Getting mmsi, imo pairs from dirty db")
        start = time.time()
        cur.execute("SELECT mmsi, imo, MIN(time), MAX(time) FROM {} WHERE message_id = 5 GROUP BY mmsi, imo".format(aisdb.dirty.get_name()))
        logging.info("Got new mmsi, imo pairs list (%fs)", time.time()-start)
        _upsert_imo_tuples(aisdb, cur, existing_tuples)

        aisdb.conn.commit()

def _upsert_imo_tuples(aisdb, result_cursor, existing_tuples):
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
        cur.execute("SELECT DISTINCT mmsi from {}".format(aisdb.squeaky.name))
        existing_mmsis = set([row[0] for row in cur.fetchall()])
        
        i = 0
        total = len(valid) - len(existing_mmsis)
        
        for mmsi, imo, start, end in intervals:
            if mmsi in existing_mmsis:
                logging.debug("MMSI %d already in table, skipping insert.", mmsi)
            else:
                i = i + 1
                cur.execute("INSERT INTO {} SELECT {} FROM {} WHERE mmsi = %s".format(aisdb.squeaky.name, ','.join([c[0].lower() for c in aisdb.squeaky.cols]), aisdb.clean.name), [mmsi])
                logging.debug("Inserted %d rows for MMSI %d. (%d/%d)", cur.rowcount, mmsi, i, total)

            aisdb.conn.commit()
