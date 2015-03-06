from repositories import sql
import logging
import psycopg2

export_commands = [('status', 'report status of this repository.'),
		('create', 'create the repository.'),
		('truncate', 'delete data in this repository.')]

def load(options, readonly=False):
	return AISRaw(options, readonly)

class AISRaw(sql.PgsqlRepository):

	double_type = 'double precision'
	cols = [('ID', 'SERIAL PRIMARY KEY'),
		('MMSI', 'integer'),
		('Time', 'timestamp without time zone'),
		('Message_ID', 'integer'),
		('Navigational_status', 'integer'),
		('SOG', double_type),
		('Longitude', double_type),
		('Latitude', double_type),
		('COG', double_type),
		('Heading', double_type),
		('IMO', 'character varying(255)'),
		('Draught', double_type),
		('Destination', 'character varying(255)'),
		('Vessel_Name', 'character varying(255)'),
		('ETA_month', 'integer'),
		('ETA_day', 'integer'),
		('ETA_hour', 'integer'),
		('ETA_minute', 'integer')]

	indices = [('dt_idx', ['Time']),
				('imo_idx', ['IMO']),
				('lonlat_idx', ['Longitude', 'Latitude']),
				('mmsi_idx', ['MMSI']),
				('msg_idx', ['Message_ID'])]

	def status(self):
		print("Status of PGSql table "+ self.db +"."+ self.getTableName() +":")
		s = self._status()
		if s >= 0:
			print("{} rows.".format(s))
		else:
			print("Table not yet created.")

	def _status(self):
		with self.conn.cursor() as cur:
			try:
				cur.execute("SELECT COUNT(*) FROM \""+ self.getTableName() +"\"")
				return cur.fetchone()[0]
			except psycopg2.ProgrammingError:
				return -1

	def create(self):
		"""Create the table for the raw AIS data."""
		with self.conn.cursor() as cur:
			logging.info("CREATING "+ self.getTableName() +" table")
			sql = "CREATE TABLE IF NOT EXISTS \""+ self.getTableName() +"\" (" + ','.join(["\"{}\" {}".format(c[0].lower(), c[1]) for c in self.cols]) + ")"
			cur.execute(sql)
			self.conn.commit()

		self._createIndices()
		
	def _createIndices(self):
		with self.conn.cursor() as cur:
			tbl = self.getTableName()
			for idx, cols in self.indices:
				idxn = tbl.lower() + "_" + idx
				try:
					logging.info("CREATING INDEX "+ idxn +" on table "+ tbl)
					cur.execute("CREATE INDEX \""+ idxn +"\" ON \""+ tbl +"\" USING btree ("+ ','.join(["\"{}\"".format(s.lower()) for s in cols]) +")" )
				except psycopg2.ProgrammingError:
					logging.info("Index "+ idxn +" already exists")
					self.conn.rollback()
			self.conn.commit()

	def _dropIndices(self):
		with self.conn.cursor() as cur:
			tbl = self.getTableName()
			for idx, cols in self.indices:
				idxn = tbl.lower() + "_" + idx
				logging.info("Dropping index: "+ idxn + " on table "+ self.getTableName())
				cur.execute("DROP INDEX IF EXISTS \""+ idxn +"\"")
			self.conn.commit()

	def truncate(self):
		"""Delete all data in the AIS table."""
		with self.conn.cursor() as cur:
			logging.info("Truncating table "+ self.getTableName())
			cur.execute("TRUNCATE TABLE \""+ self.getTableName() + "\"")
			self.conn.commit()

	def getTableName(self):
		if "tablename" in self.options:
			return self.options["tablename"]
		else:
			return "AIS_Raw"