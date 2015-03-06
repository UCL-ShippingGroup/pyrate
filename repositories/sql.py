import psycopg2

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

	def close(self):
		self.conn.close()
