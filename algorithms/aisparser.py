import os
import csv
import logging
import Queue
import threading
import time
from datetime import datetime

_algo_ = True
_type_ = "parser"
inputs = {"aiscsv": {'extensions': ['.csv']}}
outputs = {"aisraw": None, "discardlog": None}

def parseTimestamp(str):
	return datetime.strptime(str, '%Y%m%d_%H%M%S')

def intOrNull(str):
	if len(str) == 0:
		return None
	else:
		return int(str)

def floatOrNull(str):
	if len(str) == 0 or str == 'None':
		return None
	else:
		return float(str)

def imostr(str):
	if len(str) > 20:
		return None
	return str

def longstr(str):
	if len(str) > 255:
		return str.substring(0,254)
	return str

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
	files = inp['aiscsv']
	db = out['aisraw']
	log = out['discardlog']

	# drop indexes for faster insert
	db._dropIndices()

	# queue for messages to be inserted into db
	q = Queue.Queue(maxsize=10)

	# worker thread which takes batches of tuples from the queue to be
	# inserted into db
	def sqlworker():
		tuplestr = "(" + ",".join("%s" for i in ais_csv_columns) + ")"
		while True:
			msgs = q.get()

			n = len(msgs)
			with db.conn.cursor() as cur:
				# create a single query to insert list of tuples
				args = ','.join(cur.mogrify(tuplestr, x) for x in msgs)
				try:
					cur.execute("INSERT INTO \""+ db.getTableName() +"\" VALUES "+ args)
				except e:
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
		# first line is column headers. Use to extract indices
		cols = fp.readline().split(',')
		indices = []
		for c, f in ais_csv_columns:
			indices.append(cols.index(c))
		
		# open error log csv file and write header
		errorLog = open(os.path.join(log.root, name), 'wb')
		logwriter = csv.writer(errorLog, delimiter=',', quotechar='"')
		logwriter.writerow([c[0] for c in ais_csv_columns] + ["FirstError"]) 

		insertedCtr = 0
		invalidCtr = 0
		batch = []
		for row in csv.reader(fp, delimiter=',', quotechar='"'):
			try:
				convertedRow = []
				for i, col in enumerate(ais_csv_columns):
					fn = col[1] # conversion function
					raw = row[indices[i]] # raw column data
					convertedRow.append(fn(raw))
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
