import os
import csv
import logging
try:
	import queue
except ImportError:
	# python 2 compatability
	import Queue as queue
import threading
import time
from datetime import datetime

algo = True
_name_ = "aisparser"
inputs = ["aiscsv"]
outputs = ["aisraw", "discardlog"]

def parseTimestamp(s):
	return datetime.strptime(s, '%Y%m%d_%H%M%S')

def intOrNull(s):
	if len(s) == 0:
		return None
	else:
		return int(s)

def floatOrNull(s):
	if len(s) == 0 or s == 'None':
		return None
	else:
		return float(s)

def imostr(s):
	if len(s) > 20:
		return None
	return s

def longstr(s):
	if len(s) > 255:
		return s.substring(0,254)
	return s

# specifies columns to take from raw data, and functions to convert them into
# suitable type for the database.
ais_csv_columns = [('MMSI', intOrNull),
		('Time', parseTimestamp),
		('Message_ID', intOrNull),
		('Navigational_status', floatOrNull),
		('SOG', floatOrNull),
		('Longitude', floatOrNull),
		('Latitude', floatOrNull),
		('COG', floatOrNull),
		('Heading', floatOrNull),
		('IMO', longstr),
		('Draught', floatOrNull),
		('Destination', longstr),
		('Vessel_Name', longstr),
		('ETA_month', intOrNull),
		('ETA_day', intOrNull),
		('ETA_hour', intOrNull),
		('ETA_minute', intOrNull)]

def populate(inp, out, options={}):
	"""Populate the AIS_Raw database with messages from the AIS csv files."""

	files = inp['aiscsv']
	db =  out['aisraw']
	log = out['discardlog']

	# drop indexes for faster insert
	db._dropIndices()

	# queue for messages to be inserted into db
	q = queue.Queue(maxsize=10)

	# worker thread which takes batches of tuples from the queue to be
	# inserted into db
	def sqlworker():
		tuplestr = "(" + ",".join("%s" for i in ais_csv_columns) + ")"
		while True:
			msgs = q.get()

			n = len(msgs)
			if n > 0:
				with db.conn.cursor() as cur:
					# create a single query to insert list of tuples
					# note that mogrify generates a binary string which we must first decide to ascii.
					args = ','.join([cur.mogrify(tuplestr, x).decode('ascii') for x in msgs])
					try:
						cur.execute("INSERT INTO \""+ db.getTableName() +"\" VALUES "+ args)
					except Exception as e:
						logging.warning("Error executing query: {}".format(e))
			# mark this task as done
			q.task_done()
			db.conn.commit()

	t = threading.Thread(target = sqlworker)
	t.daemon = True
	t.start()

	start = time.time()

	for fp, name, ext in files.iterFiles():
		logging.info("Parsing "+ name)
		# first line is column headers. Use to extract indices of columns we are extracting
		cols = fp.readline().split(',')
		indices = []
		for c, f in ais_csv_columns:
			indices.append(cols.index(c))
		
		# open error log csv file and write header
		errorLog = open(os.path.join(log.root, name), 'w')
		logwriter = csv.writer(errorLog, delimiter=',', quotechar='"')
		logwriter.writerow([c[0] for c in ais_csv_columns] + ["FirstError"]) 

		# message counters
		insertedCtr = 0
		invalidCtr = 0
		batch = []

		# parse and iterate lines from the current csv file
		for row in csv.reader(fp, delimiter=',', quotechar='"'):
			try:
				# convert row for database insertion
				convertedRow = []
				for i, col in enumerate(ais_csv_columns):
					fn = col[1] # conversion function
					raw = row[indices[i]] # raw column data
					convertedRow.append(fn(raw))
				# add to next batch
				batch.append(convertedRow)
				insertedCtr = insertedCtr + 1
			except:
				# invalid data in row. Write it to error log
				rawRow = []
				firstError = ais_csv_columns[i][0]
				for i, col in enumerate(ais_csv_columns):
					raw = row[indices[i]] # raw column data
					rawRow.append(raw)
				logwriter.writerow(rawRow + [firstError])
				invalidCtr = invalidCtr + 1

			# submit batch to the queue
			if len(batch) >= 10000:
				q.put(batch)
				batch = []

		q.put(batch)
		errorLog.close()
		logging.info("Completed "+ name +": {} valid, {} invalid messages".format(insertedCtr, invalidCtr))

	# wait for queued tasks to finish
	q.join()

	logging.info("Parsing complete, time elapsed = {}s".format(time.time() - start))

	start = time.time()

	logging.info("Rebuilding table indices...")
	db._createIndices()
	logging.info("Finished building indices, time elapsed = {}s".format(time.time() - start))
