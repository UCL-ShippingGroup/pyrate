import os
import csv
import logging
import queue
import threading
import time
from datetime import datetime
from xml.etree import ElementTree
from pyrate import utils

algo = True
export_commands = [('run', 'parse messages from csv into the database.')]
inputs = ["aiscsv"]
outputs = ["aisdb", "baddata"]

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

def setNullOnFail(row, col, test):
	if not row[col.lower()] == None and not test(row[col.lower()]):
		row[col.lower()] = None

containsLatLon = set([1,2,3,4,9,11,17,18,19,21,27])
def latLonCheck(row):
	if row['Message_ID'.lower()] in containsLatLon:
		return utils.isValidLongitude(row['Longitude'.lower()]) and utils.isValidLatitude(row['Latitude'.lower()])
	else:
		row['Longitude'.lower()] = None
		row['Latitude'.lower()] = None

def checkIMO(imo):
	return imo is None or imo is '' or utils.isValidIMO(imo)

# specifies columns to take from raw data, and functions to convert them into
# suitable type for the database.
ais_csv_columns = [('MMSI', intOrNull),
		('Time', parseTimestamp),
		('Message_ID', intOrNull),
		('Navigational_status', intOrNull),
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

# xml names differ from csv. This array describes the names in this file which
# correspond to the csv column names
ais_xml_colnames = [
	'mmsi',
	'date_time',
	'msg_type',
	'nav_status',
	'sog',
	'lon',
	'lat',
	'cog',
	'heading',
	'imo',
	'draught',
	'destination',
	'vessel_name',
	'eta_month',
	'eta_day',
	'eta_hour',
	'eta_minute']

def rowValidator(validQ, cleanQ, dirtyQ):
	constraints = [
		('MMSI', utils.isValidMMSI),
		('Message_ID', utils.isValidMessageId),
		('IMO', checkIMO)
	]
	while True:
		row = validQ.get()
		clean = True
		# check validity constraints for malformed data
		for key, fn in constraints:
			if not fn(row[key.lower()]):
				clean = False
				#logging.debug("Failed "+key+" check, value {}".format(row[key.lower()]))
				break
		clean = clean and latLonCheck(row)
		if not clean:
			dirtyQ.put(row)
		else:
			# clean data for clean db
			setNullOnFail(row, 'Navigational_status', utils.isValidNavigationalStatus)
			setNullOnFail(row, 'SOG', utils.isValidSOG)
			setNullOnFail(row, 'COG', utils.isValidCOG)
			setNullOnFail(row, 'Heading', utils.isValidHeading)
			cleanQ.put(row)

		validQ.task_done()

def run(inp, out, options={}):
	"""Populate the AIS_Raw database with messages from the AIS csv files."""

	files = inp['aiscsv']
	db =  out['aisdb']
	log = out['baddata']

	# drop indexes for faster insert
	db.clean.dropIndices()
	db.dirty.dropIndices()

	# queue for messages to be inserted into db
	validQ = queue.Queue(maxsize=5000)
	dirtyQ = queue.Queue(maxsize=5000)
	cleanQ = queue.Queue(maxsize=5000)

	# worker thread which takes batches of tuples from the queue to be
	# inserted into db
	def sqlworker(q, table):
		cols = [c[0] for c in ais_csv_columns]
		while True:
			msgs = [q.get()]
			while not q.empty():
				msgs.append(q.get(timeout=0.5))

			n = len(msgs)
			if n > 0:
				#logging.debug("Inserting {} rows into {}".format(n, table.name))
				try:
					table.insertRowsBatch(msgs)
				except Exception as e:
					logging.warning("Error executing query: {}".format(e))
			# mark this task as done
			for i in range(n):
				q.task_done()
			db.conn.commit()

	# set up processing pipeline threads
	cleanThread = threading.Thread(target = sqlworker, daemon=True, args=(cleanQ, db.clean))
	dirtyThread = threading.Thread(target = sqlworker, daemon=True, args=(dirtyQ, db.dirty))
	validThread = threading.Thread(target = rowValidator, daemon=True, args=(validQ, cleanQ, dirtyQ))
	
	validThread.start()
	dirtyThread.start()
	cleanThread.start()

	start = time.time()

	for fp, name, ext in files.iterFiles():
		# check if we've already parsed this file
		with db.conn.cursor() as cur:
			cur.execute("SELECT COUNT(*) FROM " + db.sources.name + " WHERE filename = %s", [name])
			if cur.fetchone()[0] > 0:
				logging.info("Already parsed "+ name +", skipping...")
				continue

		logging.info("Parsing "+ name)
		
		# open error log csv file and write header
		errorLog = open(os.path.join(log.root, name), 'w')
		logwriter = csv.writer(errorLog, delimiter=',', quotechar='"')
		logwriter.writerow([c[0] for c in ais_csv_columns] + ["FirstError", "Error_Message"]) 

		# message counters
		totalCtr = 0
		invalidCtr = 0

		# Select the a file iterator based on file extension
		if ext == '.csv':
			iterator = readcsv
		elif ext == '.xml':
			iterator = readxml
		else:
			logging.warning("Cannot parse file with extension {}".format(ext))
			continue

		# parse and iterate lines from the current file
		for row in iterator(fp):
			try:
				# convert row for database insertion
				convertedRow = {}
				for i, col in enumerate(ais_csv_columns):
					fn = col[1] # conversion function
					convertedRow[col[0].lower()] = fn(row[i])
				convertedRow['source'] = 0
				# add to next batch
				validQ.put(convertedRow)
			except Exception as e:
				# invalid data in row. Write it to error log
				firstError = ais_csv_columns[i][0]
				logwriter.writerow(row + [firstError, "{}".format(e)])
				invalidCtr = invalidCtr + 1

			totalCtr = totalCtr + 1

		db.sources.insertRow({'filename': name, 'ext': ext, 'invalid': invalidCtr, 'total': totalCtr})

		errorLog.close()
		logging.info("Completed "+ name +": {} valid, {} invalid messages".format(totalCtr, invalidCtr))

	# wait for queued tasks to finish
	validQ.join()
	dirtyQ.join()
	cleanQ.join()
	db.conn.commit()

	logging.info("Parsing complete, time elapsed = {}s".format(time.time() - start))

	start = time.time()

	logging.info("Rebuilding table indices...")
	db.clean.createIndices()
	db.dirty.createIndices()
	logging.info("Finished building indices, time elapsed = {}s".format(time.time() - start))

def readcsv(fp):
	# first line is column headers. Use to extract indices of columns we are extracting
	cols = fp.readline().split(',')
	indices = []
	try:
		for c, f in ais_csv_columns:
			indices.append(cols.index(c))
	except Exception as e:
		raise RuntimeError("Missing columns in file header: {}".format(e))

	for row in csv.reader(fp, delimiter=',', quotechar='"'):
		rowsubset = []
		for i, col in enumerate(ais_csv_columns):
			raw = row[indices[i]] # raw column data
			rowsubset.append(raw)
		yield rowsubset

def readxml(fp):

	current = ["" for i in ais_xml_colnames]
	# iterate xml 'end' events
	for event, elem in ElementTree.iterparse(fp):
		# end of aismessage
		if elem.tag == 'aismessage':
			yield current
			current = ["" for i in ais_xml_colnames]
		else:
			if elem.tag in ais_xml_colnames and elem.text != None:
				current[ais_xml_colnames.index(elem.tag)] = elem.text
