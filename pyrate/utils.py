import datetime
import pandas as pd
import numpy

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
    return lon != None and lon >= -180 and lon <= 180

def valid_latitude(lat):
    return lat != None and lat >= -90 and lat <= 90

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
    try:
        return (heading >= 0 and heading < 360) or heading == 511
    except:
        #getting errors on none type for heading
        return False

def speed_calc(msg_stream, index1, index2):
    timediff = abs(msg_stream[index2]['Time'] - msg_stream[index1]['Time'])
    try:
        dist = distance((msg_stream[index1]['Latitude'], msg_stream[index1]['Longitude']),\
                       (msg_stream[index2]['Latitude'], msg_stream[index2]['Longitude'])).m
    except ValueError:
        dist = Geodesic.WGS84.Inverse(msg_stream[index1]['Latitude'], msg_stream[index1]['Longitude'],\
                       msg_stream[index2]['Latitude'], msg_stream[index2]['Longitude'])['s12'] #in metres
    if timediff > datetime.timedelta(0):
        speed = (dist*0.0005399568)/(timediff.days*24 + timediff.seconds/3600)
    else:
        speed = 102.2
    return timediff, dist, speed

def detect_location_outliers(msg_stream, as_df=False):
    """
    1)  Create a linked list of all messages with non-null locations (pointing to next message)
    2)  Loop through linked list and check for location outliers:
        A location outlier is who does not pass the speed test (<= 50kn; link is 'discarded' when not reached in time)
        No speed test is performed when
            - distance too small (< 0.054nm ~ 100m; catches most positioning inaccuracies)
                => no outlier
            - time gap too big (>= 215h ~ 9d; time it takes to get anywhere on the globe at 50kn not respecting land)
                =>  next message is new 'start'
        If an alledged outlier is found its link is set to be the current message's link
    3)  The start of a linked list becomes special attention: if speed check fails, the subsequent link is tested
    
    Line of thinking is: Can I get to the next message in time? If not 'next' must be an outlier, go to next but one.
    """
    from geographiclib.geodesic import Geodesic
    from geopy.distance import distance
    if as_df:
        raise NotImplementedError('msg_stream cannot be DataFrame, as_df=True does not work yet.')

    # 1) linked list
    linked_rows = [None]*len(msg_stream)
    link = None
    has_location_count = 0
    for row_index in reversed(range(len(msg_stream))):
        if msg_stream[row_index]['Longitude'] is not None and msg_stream[row_index]['Latitude'] is not None:
            linked_rows[row_index] = link
            link = row_index
            has_location_count = has_location_count + 1
    # 2)
    outlier_rows = [False] * len(msg_stream)
    if has_location_count < 2: # no messages that could be outliers available
        return outlier_rows

    index = next((i for i, j in enumerate(linked_rows) if j), None)
    at_start = True

    while linked_rows[index] is not None:
        # time difference, distance and speed beetween rows of index and corresponding link
        timediff, dist, speed = speed_calc(msg_stream, index, linked_rows[index])

        if timediff > datetime.timedelta(hours=215):
            index = linked_rows[index]
            at_start = True #restart
        elif  dist < 100:
            index = linked_rows[index] #skip over gap (at_start remains same)
        elif speed > 50:
            if at_start is False:
                #for now just skip single outliers, i.e. test current index with next but one
                outlier_rows[linked_rows[index]] = True
                linked_rows[index] = linked_rows[linked_rows[index]]
            elif at_start is True and linked_rows[linked_rows[index]] is None: #no subsequent message
                outlier_rows[index] = True
                outlier_rows[linked_rows[index]] = True
                index = linked_rows[index]
            else: # explore first three messages A, B, C (at_start is True)
                indexA = index
                indexB = linked_rows[index]
                indexC = linked_rows[indexB]

                timediffAC, distAC, speedAC = speed_calc(msg_stream, indexA, indexC)
                timediffBC, distBC, speedBC = speed_calc(msg_stream, indexB, indexC)

                #if speedtest A->C ok or distance small, then B is outlier, next index is C
                if timediffAC <= datetime.timedelta(hours=215) and (distAC < 100 or speedAC <= 50):
                    outlier_rows[indexB] = True
                    index = indexC
                    at_start = False

                #elif speedtest B->C ok or distance, then A is outlier, next index is C
                elif timediffBC <= datetime.timedelta(hours=215) and (distBC < 100 or speedBC <= 50):
                    outlier_rows[indexA] = True
                    index = indexC
                    at_start = False

                #else bot not ok, dann A and B outlier, set C to new start
                else:
                    outlier_rows[indexA] = True
                    outlier_rows[indexB] = True
                    index = indexC
                    at_start = True
        else: #all good
            index = linked_rows[index]
            at_start = False

    return outlier_rows

def interpolate_passages(msg_stream):
    """Interpolate far apart points in an ordered stream of messages.

    Returns list of artificial messages to fill in gaps/navigate around land.

    msg_stream is a list of dictionaries representing AIS messages for a single
    MMSI number. Dictionary keys correspond to the column names from the
    ais_clean table. The list of messages should be ordered by timestamp in
    ascending order.
    """
    return []
