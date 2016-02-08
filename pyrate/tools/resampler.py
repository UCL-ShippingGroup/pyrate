import pandas as pd
import numpy

def convert_messages_to_hourly_bins(df, period='H', fillnans=False,
                                    run_resample=True):
    """Resample the messages to a new time-resolution.

    Defaults to hourly.

    Arguments
    ---------
    df : pandas DataFrame
        A DataFrame of messages
    period : string, optional
        Indicates the period to resample over
    fillnans : bool, optional
        Defaults to False
    run_resample : bool, optional
        Defaults to True

    Notes
    -----
    Intended for use with the extended database

    Called internally, one of the wrapper functions should be called

    """
    if df.empty:
        return df

    if run_resample:

        speed_ts = df.sog.resample(period,how='mean')

        draught_ts = df.draught.resample(period,how=numpy.max)
        df_new = pd.DataFrame({'sog':speed_ts,'draught':draught_ts})

        for col in df.columns:
            if col != 'sog' and col!='draught':
                df_new[col] = df[col].resample(period, how='first')

    else:
        df_new=[]

    #set the time equal to the index
    df_new['time'] = df_new.index.values
    # fill forward
    if fillnans:
        #forward fill first
        df_new = df_new.fillna(method='pad')
        #now backward fill for remain
        df_new = df_new.fillna(method='bfill')
    else:
        #remove all entries where there are nans in speed
        df_new = df_new.ix[pd.isnull(df_new.sog) == False]
    return df_new
