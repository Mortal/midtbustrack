'''One-off program to convert old store layout to new store layout'''
import datetime
import pandas as pd

from fetch import bus_key


def main():
    attr_keys = '''
    Id Name StartStation EndStation StartName EndName StartTime EndTime
    DirectionText'''.split()
    rec_keys = 'Updated Delay Lat Lon'.split()
    with pd.HDFStore('input.h5') as input, pd.HDFStore('output.h5') as output:
        groups = input['buses'].groupby('Line EndStation Id JourneyId'.split())
        for (line, endstation, id, journey), df in groups:
            date = df.StartTime.iloc[0].date()
            key = bus_key(dict(
                Line=line, EndStation=endstation, Id=id, JourneyId=journey,
                StartTime=datetime.datetime.combine(date, datetime.time())))
            attr = {k: df[k].iloc[0] for k in attr_keys}
            output.put(key, df[rec_keys].set_index(df.request_time),
                       format='table')
            output.get_storer(key).attrs.metadata = attr


if __name__ == '__main__':
    main()
