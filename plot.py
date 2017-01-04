from collections import Counter

import datetime
import numpy as np
import pyproj
import pandas as pd
import matplotlib.pyplot as plt

from fetch import timestamp


CLOSE_THRESHOLD = 1e-5


def project(df):
    index = df.index
    sproj = pyproj.Proj('+init=epsg:3857')
    dproj = pyproj.Proj('+proj=utm +zone=32 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
    x, y = pyproj.transform(sproj, dproj, df.Lon.values, df.Lat.values)
    return pd.Series(x, index=index), pd.Series(y, index=index)


def show_all_buses(store):
    buses = store['buses'].reset_index()
    where_2A = buses['Line'] == '2A'
    where_new = buses['request_time'] >= timestamp('2017-01-03 16:06:46')
    show_buses(buses[where_2A & where_new], 'g')
    plt.show()


def show_buses(buses, color):
    journeys = buses.groupby('JourneyId')
    for journey_id, points in journeys:
        plt.plot(*project(points), '-' + color)
    plt.axis('equal')


def find_most_recent_bus(store, utm32_position, now, line='2A', end_station=751421800):
    buses = store['buses'].reset_index()
    buses = buses[buses.request_time >= timestamp('2017-01-03 16:06:46')]
    buses = buses[buses.Line == line]
    buses = buses[buses.EndStation == end_station]
    buses = buses[buses.request_time <= now]
    times = predict_bus_times(buses, utm32_position, now)
    print(times)


def predict_bus_times(buses, utm32_position, now):
    return predict_bus_times_multi(buses, utm32_position, [now])[0]


def predict_bus_times_multi(buses, utm32_position, nows):
    journeys = buses.groupby('JourneyId')
    # print('%s journeys' % len(journeys))
    x, y = utm32_position
    buses_x, buses_y = project(buses)
    dx = x - buses_x
    dy = y - buses_y
    dist_sq = dx ** 2 + dy ** 2
    journey_dist_sq = pd.DataFrame(
        dict(dist_sq=dist_sq, JourneyId=buses.JourneyId))
    journey_dist_sq = journey_dist_sq.groupby('JourneyId').dist_sq
    journey_is_close = journey_dist_sq.min() < CLOSE_THRESHOLD
    close_journey_ids = journey_is_close[journey_is_close].index
    # print(close_journey_ids)
    # close_journeys_series = buses[buses.JourneyId.isin(close_journey_ids)]
    pass_by_idx = journey_dist_sq.idxmin().loc[close_journey_ids]
    # print(pass_by_idx)
    pass_by_point = buses.loc[pass_by_idx]
    pass_by_time = pass_by_point.request_time
    most_recent_point = buses.loc[pass_by_time.argmax()]
    most_recent_journey = most_recent_point.JourneyId
    most_recent = buses[buses.JourneyId == most_recent_journey]
    most_recent_x, most_recent_y = project(most_recent)

    # print(most_recent_point)
    # show_buses(most_recent, 'k')

    journey_is_far = ~journey_is_close
    far_journey_ids = journey_is_far[journey_is_far].index
    far_journeys_idx = journeys.request_time.idxmax().loc[far_journey_ids]
    far_journeys_point = buses.loc[far_journeys_idx].set_index('JourneyId')
    far_journeys_x, far_journeys_y = project(far_journeys_point)
    # print(far_journeys_point)

    result = []
    for now in nows:
        result_now = {}
        recent_threshold = now - datetime.timedelta(seconds=60)
        for journey_id in far_journey_ids:
            t = far_journeys_point.loc[journey_id].Updated
            if t < recent_threshold:
                # print('Skip %s at time %s' % (journey_id, t))
                continue
            journey = buses[buses.JourneyId == journey_id]
            dx = far_journeys_x[journey_id] - most_recent_x
            dy = far_journeys_y[journey_id] - most_recent_y
            dist_sq = dx ** 2 + dy ** 2
            closest = dist_sq.idxmin()
            closest_time = buses.loc[closest].Updated
            t_diff = t - closest_time
            predicted = most_recent_point.Updated + t_diff
            until = predicted - now
            # print('%s is behind by %s, expected in %s at %s' %
            #       (journey_id, t_diff, until, predicted))
            result_now[journey_id] = predicted
            # show_buses(journey, '')
            # plt.plot([far_journeys_x[journey_id], most_recent_x[closest]],
            #          [far_journeys_y[journey_id], most_recent_y[closest]], 'x-b')
        result.append(result_now)
    return result


def main():
    with pd.HDFStore('midtbustrack.h5') as store:
        # show_all_buses(store)
        find_most_recent_bus(store, (-505636.59, 56.4725), datetime.datetime.now())


if __name__ == '__main__':
    main()
