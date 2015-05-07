from geographiclib.geodesic import Geodesic
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
    max_allow_speed_nm_hr=50
    
    
    def get_dist(row):
        #print row
        #try:
            straight_line=Geodesic.WGS84.Inverse(row['Latitude'],row['Longitude'],\
                row['next_lat'],row['next_lon'])
            dist=straight_line['s12']/1000
            return dist
        #except:
        #    return float('inf')
            
            
    df_orig=pd.DataFrame.from_records(msg_stream)
    df_dist=pd.DataFrame.from_records(msg_stream)
    
    #calculate the required speed between data points
    df_dist.sort('Time',inplace=True)    
    df_dist.reset_index(inplace=True,drop=True)
    df_dist['next_lat']=df_dist['Latitude'].shift(periods=1)
    df_dist['next_lon']=df_dist['Longitude'].shift(periods=1)
    df_dist['dist']=df_dist.apply(get_dist,axis=1)
    df_dist['elapsed_time']=df_dist.Time.diff(1)
    
    
    def get_speed(row):
        if (row.elapsed_time==None)|(row.elapsed_time==0.0):
            return 0.0
        else:
            return row['dist']/(row.elapsed_time/(60*60))
        
    #get speed between consecutive points
    df_dist.elapsed_time=df_dist.elapsed_time.astype('timedelta64[s]')
    df_dist['required_speed']=df_dist.apply(get_speed,axis=1)    
    
    print(df_dist.head())
    
    df_dist['location_flag']=0
    df_dist['location_flag'].ix[\
            df_dist.required_speed*1.852>max_allow_speed_nm_hr]=1
    df_dist['orig_loc_flag']=df_dist['location_flag'].values
    #Identify the index which was the source for the speed of greater than 50
    df_dist['start_flag_index']=0        
    df_dist['start_flag_index'].ix[df_dist.location_flag.diff(-1)==-1]=1
    #Identify if following messages were also outliers
    #i.e. we need to get the speed from the source message not the outlier
    #walk forward from each location flag until we find the datapoint that
    #is acceptable
    for jj,start_indx in enumerate(df_dist.ix[df_dist.start_flag_index==1].index):
        print("%d of %d"%(jj,len(df_dist.ix[df_dist.start_flag_index==1].index)))
        for ii in range(start_indx+1,df_dist.shape[0]):
            try:
                straight_line=Geodesic.WGS84.Inverse(df_dist['Latitude'].ix[ii],\
                    df_dist['Longitude'].ix[ii],\
                    df_dist['Latitude'].ix[start_indx],\
                    df_dist['Longitude'].ix[start_indx])
                dist=straight_line['s12']/(1000*1.852)
            except:
                dist=float('inf')
            if pd.isnull(dist):
                dist=float('inf')
            #df_dist['st_dist'].ix[ii]=dist
            elapsed_time=(df_dist['Time'].ix[ii]-\
                df_dist['Time'].ix[start_indx]).seconds
            speed=dist/(numpy.max([1.0,elapsed_time])/(60.0*60))
            if speed>max_allow_speed_nm_hr:
                df_dist.location_flag.ix[ii]=1
            else:
                df_dist.location_flag.ix[ii]=0
                break
    
     
    #Now do speed flag
    df_dist['speed_flag']=0
    df_dist['speed_flag'].ix[pd.isnull(df_dist.SOG)|(df_dist.SOG>102.2)]=1
    
    # Now do course flag
    df_dist['speed_flag']=0
    df_dist['course_flag'].ix[pd.isnull(df_dist.COG)|\
        (df_dist.COG>=360.0)|(df_dist.COG<0.0)]=1
    
    # Now do heaidng flag
    df_dist['heading_flag']=0
    df_dist['heading_flag'].ix[pd.isnull(df_dist.Heading)|\
        (df_dist.Heading>=360.0)|(df_dist.Heading<0.0)]=1
    
    # Now do nav status flag
    df_dist['nav_status_flag']=0
    df_dist['nav_status_flag'].ix[\
        df_dist.Navigational_status.isin([0,1,2,3,4,5,6,7,8,10,12,14])==False]=1
        
    #eta - don't need to check eta
#    def check_eta(row):
#        eta_chk=clean_eta(row,year=row.date_time)
#        if eta_chk==None:
#            return 1
#        if eta_chk>row.date_time:
#            return 0
#        else:
#            return 1
#    df_dist['eta_flag']=df_dist.apply(check_eta,axis=1)
#    df_dist['eta']=None
#    df_dist['eta'].ix[df_dist.eta_flag==0]=df_dist.apply(clean_eta,axis=1)
#    df_dist['eta']=pd.to_datetime(df_dist['eta'])
    
    #get indexes where the sum of the flags is zero
    sum_flagged=df_dist[['speed_flag','speed_flag',\
        'heading_flag','nav_status_flag','location_flag']].sum(axis=1)
    unflag_indxs=df_dist.ix[sum_flagged==0].index
    flag_indxs=df_dist.ix[sum_flagged>0].index
    return [msg_stream[x] for x in unflag_indxs], \
        [msg_stream[x] for x in flag_indxs]

def interpolate_passages(msg_stream):
    """Interpolate far apart points in an ordered stream of messages.

    Returns list of artificial messages to fill in gaps/navigate around land.

    msg_stream is a list of dictionaries representing AIS messages for a single
    MMSI number. Dictionary keys correspond to the column names from the
    ais_clean table. The list of messages should be ordered by timestamp in
    ascending order.
    """
    return []