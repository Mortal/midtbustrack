from collections import Counter

import datetime
import numpy as np
import pyproj
import pandas as pd
import matplotlib.pyplot as plt

from fetch import timestamp


def project(lon, lat):
    sproj = pyproj.Proj('+init=epsg:3857')
    dproj = pyproj.Proj('+proj=utm +zone=32 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
    return pyproj.transform(sproj, dproj, lon, lat)


def show_all_buses(store):
    buses = store['buses'].reset_index()
    where_2A = buses['Line'] == '2A'
    where_new = buses['request_time'] >= timestamp('2017-01-03 16:06:46')
    show_buses(buses[where_2A & where_new])
    plt.show()


def show_buses(buses):
    journeys = buses.groupby('JourneyId')
    for journey_id, points in journeys:
        plt.plot(*project(points['Lon'].values, points['Lat'].values), '.-')
    plt.axis('equal')


def find_most_recent_bus(store, utm32_position, now, line='2A', end_station=751421800):
    buses = store['buses'].reset_index()
    buses = buses[buses['request_time'] >= timestamp('2017-01-03 16:06:46')]
    buses = buses[buses.Line == line]
    buses = buses[buses.EndStation == end_station]
    buses = buses[buses.request_time <= now]
    print('%s journeys' % len(buses.groupby('JourneyId')))
    x, y = utm32_position
    buses['x'], buses['y'] = project(
        buses['Lon'].values, buses['Lat'].values)
    dx = x - buses['x'].values
    dy = y - buses['y'].values
    buses['dist_sq'] = dx ** 2 + dy ** 2
    show_buses(buses)
    journeys = buses.groupby('JourneyId')
    print(journeys['dist_sq'].min())
    for journey_id, df in journeys:
        #print(df)
        #print('dist_sq for journey %s' % journey_id)
        #print(type(df['dist_sq']))
        #print(df['dist_sq'])
        ix = df['dist_sq'].idxmin()
        #print(ix, len(df['dist_sq']), len(df))
        x2, y2 = df['x'].loc[ix], df['y'].loc[ix]
        #print((x - x2) ** 2 + (y - y2) ** 2)
        plt.plot([x, x2], [y, y2], 'x-k')
    #closest = np.argmin(dist_sq)
    #print(buses.iloc[closest])
    #x2, y2 = utm32_points[closest]
    plt.show()


def main():
    with pd.HDFStore('midtbustrack.h5') as store:
        # show_all_buses(store)
        find_most_recent_bus(store, (-505636.59, 56.4725), datetime.datetime.now())


if __name__ == '__main__':
    main()
