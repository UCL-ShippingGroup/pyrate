import psycopg2
import logging

def load(options, readonly=False):
	return PgsqlRepository(host, database, user, password)

class PgsqlRepository:

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
		
	def __enter__(self):
		self.conn = psycopg2.connect(host=self.host, database=self.db, user=self.user, password=self.password)

	def __exit__(self, exc_type, exc_value, traceback):
		self.conn.close()

class Table:

	def __init__(self, db, name, cols, indices = []):
		self.db = db
		self.name = name
		self.cols = cols
		self.indices = indices

	def getName(self):
		return self.name

	def create(self):
		with self.db.conn.cursor() as cur:
			logging.info("CREATING "+ self.name +" table")
			sql = "CREATE TABLE IF NOT EXISTS \""+ self.name +"\" (" + ','.join(["\"{}\" {}".format(c[0].lower(), c[1]) for c in self.cols]) + ")"
			cur.execute(sql)
			self.db.conn.commit()

		self.createIndices()

	def createIndices(self):
		with self.db.conn.cursor() as cur:
			tbl = self.name
			for idx, cols in self.indices:
				idxn = tbl.lower() + "_" + idx
				try:
					logging.info("CREATING INDEX "+ idxn +" on table "+ tbl)
					cur.execute("CREATE INDEX \""+ idxn +"\" ON \""+ tbl +"\" USING btree ("+ ','.join(["\"{}\"".format(s.lower()) for s in cols]) +")" )
				except psycopg2.ProgrammingError:
					logging.info("Index "+ idxn +" already exists")
					self.db.conn.rollback()
			self.db.conn.commit()

	def dropIndices(self):
		with self.db.conn.cursor() as cur:
			tbl = self.name
			for idx, cols in self.indices:
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

	def insertRow(self, data, cols = None):
		with self.db.conn.cursor() as cur:
			if cols != None:
				columnlist = '(' + ','.join(cols) + ')'
			else:
				columnlist = ''
			tuplestr = "(" + ",".join("%s" for i in data) + ")"
			cur.execute("INSERT INTO " + self.name + " "+ columnlist + " VALUES "+ tuplestr, data)

	def insertRowsBatch(self, rows, cols = None):
		with self.db.conn.cursor() as cur:
			if cols != None:
				columnlist = '(' + ','.join(cols) + ')'
			else:
				columnlist = ''
			tuplestr = "(" + ",".join("%s" for i in rows[0]) + ")"
			# create a single query to insert list of tuples
			# note that mogrify generates a binary string which we must first decode to ascii.
			args = ','.join([cur.mogrify(tuplestr, x).decode('ascii') for x in rows])
			cur.execute("INSERT INTO " + self.name + " "+ columnlist + " VALUES "+ args)
