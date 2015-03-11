
def isValidMMSI(mmsi):
	"""Checks if a given MMSI number is valid. Returns true if mmsi number is 9 digits long."""
	return len(str(int(mmsi))) == 9

validMessageIds = range(1,28)
def isValidMessageId(messageId):
	return messageId in validMessageIds

validNavigationalStatuses = set([0,1,2,3,4,5,6,7,8,11,12,14,15])
def isValidNavigationalStatus(status):
	return status in validNavigationalStatuses

def isValidLongitude(lon):
	return lon >= -180 and lon <= 180

def isValidLatitude(lat):
	return lat >= -90 and lat <= 90

def isValidIMO(imo=0):
	"""Check valid IMO using checksum.

	Taken from Eoin O'Keeffe's checksum_valid function in pyAIS"""
	try:
		str_imo=str(int(imo))
		sum_val=0
		for ii,chk in enumerate(range(7,1,-1)):
			sum_val+=chk*int(str_imo[ii])
		if str_imo[6]==str(sum_val)[len(str(sum_val))-1]:
			return True
	except:
		return False
	return False

def isValidSOG(sog):
	return sog >= 0 and sog <= 102.2

def isValidCOG(cog):
	return cog >= 0 and cog <= 360

def isValidHeading(heading):
	return (heading >= 0 and heading < 360) or heading == 511
