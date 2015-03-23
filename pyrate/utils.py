
def valid_mmsi(mmsi):
    """Checks if a given MMSI number is valid. Returns true if mmsi number is 9 digits long."""
    return not mmsi is None and len(str(int(mmsi))) == 9

VALID_MESSAGE_IDS = range(1, 28)
def valid_message_id(message_id):
    return message_id in VALID_MESSAGE_IDS

VALID_NAVIGATIONAL_STATUSES = set([0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 14, 15])
def valid_navigational_status(status):
    return status in VALID_NAVIGATIONAL_STATUSES

def valid_longitude(lon):
    return lon >= -180 and lon <= 180

def valid_latitude(lat):
    return lat >= -90 and lat <= 90

def valid_imo(imo=0):
    """Check valid IMO using checksum.

    Taken from Eoin O'Keeffe's checksum_valid function in pyAIS"""
    try:
        str_imo = str(int(imo))
        if len(str_imo) != 7:
        	return False
        sum_val = 0
        for ii, chk in enumerate(range(7, 1, -1)):
            sum_val += chk * int(str_imo[ii])
        if str_imo[6] == str(sum_val)[len(str(sum_val))-1]:
            return True
    except:
        return False
    return False

def is_valid_sog(sog):
    return sog >= 0 and sog <= 102.2

def is_valid_cog(cog):
    return cog >= 0 and cog <= 360

def is_valid_heading(heading):
    return (heading >= 0 and heading < 360) or heading == 511

def detect_outliers(msg_stream):
    """Detect erroneous points in an ordered stream of messages.

    Returns a tuple of: 
     * list of valid messages from the stream;
     * list of discarded messages from the stream.

    msg_stream is a list of dictionaries representing AIS messages for a single
    MMSI number. Dictionary keys correspond to the column names from the
    ais_clean table. The list of messages should be ordered by timestamp in
    ascending order.
    """
    return msg_stream, []

def interpolate_passages(msg_stream):
    """Interpolate far apart points in an ordered stream of messages.

    Returns list of artificial messages to fill in gaps/navigate around land.

    msg_stream is a list of dictionaries representing AIS messages for a single
    MMSI number. Dictionary keys correspond to the column names from the
    ais_clean table. The list of messages should be ordered by timestamp in
    ascending order.
    """
    return []