import psycopg2
import logging

def load(options, readonly=False):
    return PgsqlRepository(options)

class PgsqlRepository(object):

    def __init__(self, options, readonly=False):
        self.options = options
        self.host = options['host']
        self.db = options['db']
        if readonly:
            self.user = options['ro_user']
            self.password = options['ro_pass']
        else:
            self.user = options['user']
            self.password = options['pass']
        self.conn = None

    def connection(self):
        return psycopg2.connect(host=self.host, database=self.db, user=self.user, password=self.password)

    def __enter__(self):
        self.conn = self.connection()

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

class Table(object):

    def __init__(self, db, name, cols, indices=None, constraint=None,
                 foreign_keys=None):
        self.db = db
        self.name = name
        self.cols = cols
        self.indices = indices
        self.foreign_keys = foreign_keys
        if self.foreign_keys is None:
            self.foreign_keys = []
        if self.indices is None:
            self.indices = []
        self.constraint = constraint
        if self.constraint is None:
            self.constraint = []

    def get_name(self):
        return self.name

    def create(self):
        with self.db.conn.cursor() as cur:
            logging.info("CREATING " + self.name + " table")
            columns = []
            fks = [x[0] for x in self.foreign_keys]
            for c in self.cols:
                if c[0].lower() not in fks:
                    columns.append("\"{}\" {}".format(c[0].lower(), c[1]))
                else:
                    fk = [x for x in self.foreign_keys if x[0] == c[0]]
                    columns.append("\"{}\" {} REFERENCES {} (\"{}\")".format(
                        c[0].lower(), c[1], fk[0][1], fk[0][2]))
            # columns = ["\"{}\" {}".format(c[0].lower(),
            # c[1]) for c in self.cols]
            sql = "CREATE TABLE IF NOT EXISTS \"" + self.name + \
                "\" (" + ','.join(columns + self.constraint) + ")"
            cur.execute(sql)
            self.db.conn.commit()

        self.create_indices()

    def create_indices(self):
        with self.db.conn.cursor() as cur:
            tbl = self.name
            for idx, cols in self.indices:
                idxn = tbl.lower() + "_" + idx
                try:
                    logging.info("CREATING INDEX "+ idxn +" on table "+ tbl)
                    cur.execute("CREATE INDEX \""+ idxn +"\" ON \""+ tbl +"\" USING btree ("+ ','.join(["\"{}\"".format(s.lower()) for s in cols]) +")")
                except psycopg2.ProgrammingError:
                    logging.info("Index "+ idxn +" already exists")
                    self.db.conn.rollback()
            self.db.conn.commit()

    def drop_indices(self):
        with self.db.conn.cursor() as cur:
            tbl = self.name
            for idx, _ in self.indices:
                idxn = tbl.lower() + "_" + idx
                logging.info("Dropping index: "+ idxn + " on table "+ tbl)
                cur.execute("DROP INDEX IF EXISTS \""+ idxn +"\"")
            self.db.conn.commit()

    def truncate(self):
        """Delete all data in the table."""
        with self.db.conn.cursor() as cur:
            logging.info("Truncating table "+ self.name)
            cur.execute("TRUNCATE TABLE \""+ self.name + "\"")
            self.db.conn.commit()

    def status(self):
        with self.db.conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM \""+ self.name +"\"")
                self.db.conn.commit()
                return cur.fetchone()[0]
            except psycopg2.ProgrammingError:
                self.db.conn.rollback()
                return -1

    def insert_row(self, data):
        with self.db.conn.cursor() as cur:
            columnlist = '(' + ','.join([c.lower() for c in data.keys()]) + ')'
            tuplestr = "(" + ",".join("%({})s".format(i) for i in data.keys()) + ")"
            cur.execute("INSERT INTO " + self.name + " "+ columnlist + " VALUES "+ tuplestr, data)

    def insert_rows_batch(self, rows):
        # check there are rows in insert
        if len(rows) == 0:
            return
        with self.db.conn.cursor() as cur:
            columnlist = '(' + ','.join([c.lower() for c in rows[0].keys()]) + ')'
            tuplestr = "(" + ",".join("%({})s".format(i) for i in rows[0]) + ")"
            # create a single query to insert list of tuples
            # note that mogrify generates a binary string which we must first decode to ascii.
            args = ','.join([cur.mogrify(tuplestr, x).decode('ascii') for x in rows])
            cur.execute("INSERT INTO " + self.name + " "+ columnlist + " VALUES "+ args)

    def copy_from_file(self, fname, columns):
        with self.db.conn.cursor() as cur:
            cur.execute("COPY "+ self.name + " (" + ','.join(c.lower() for c in columns) +") FROM %s DELIMITER ',' CSV HEADER", [fname])
