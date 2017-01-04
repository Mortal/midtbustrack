from collections import Counter

import datetime
import numpy as np
import pyproj
import pandas as pd
import matplotlib.pyplot as plt

from fetch import timestamp


RADIUS_SQ = 1e-5


def project(df):
    index = df.index
    sproj = pyproj.Proj('+init=epsg:3857')
    dproj = pyproj.Proj('+proj=utm +zone=32 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
    x, y = pyproj.transform(sproj, dproj, df.Lon.values, df.Lat.values)
    return (pd.Series(x, index=index, name='x'),
            pd.Series(y, index=index, name='y'))


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


auh = 751421800  # AUH i Skejby
holme = 751435500  # Holme (Karetmagertoften)


def get_buses(store, line='2A', end_station=auh):
    buses = store['buses'].reset_index()
    buses = buses[buses.request_time >= timestamp('2017-01-03 16:06:46')]
    buses = buses[buses.Line == line]
    buses = buses[buses.EndStation == end_station]
    return buses


def find_most_recent_bus(buses, utm32_position, now):
    # buses = buses[buses.request_time <= now]
    times = predict_bus_times(buses, utm32_position, now)
    for k in sorted(times.keys(), key=lambda k: times[k]):
        print('%s %s' % (times[k], k))


def predict_bus_times(buses, utm32_position, now):
    (result,), labels = predict_bus_times_multi(buses, utm32_position, [now])
    # *foo, bar = ([x], atom)
    # foo = [[x]]
    # bar = atom

    print(now)
    labels_dict = {i: labels.loc[i] for i in labels.index}
    for journey_id in sorted(result.keys(), key=lambda k: result[k]):
        predicted = result[journey_id]
        actual = labels_dict.get(journey_id)
        if actual:
            predicted_s = (predicted - now).total_seconds()
            actual_s = (actual - now).total_seconds()
            abs_diff = predicted_s - actual_s
            rel_diff = predicted_s / actual_s - 1
            diff_str = ' (%+.0f s, %.0f%%)' % (abs_diff, 100*rel_diff)
        else:
            diff_str = ''
        print('%s %s%s' % (predicted, actual, diff_str))
    return result


def plot_prediction(buses, utm32_position, now):
    hours = 10
    nows = [now + datetime.timedelta(seconds=s)
            for s in range(-3600*hours, 0, 60)]
    result, labels = predict_bus_times_multi(buses, utm32_position, nows)
    labels_dict = {i: labels.loc[i] for i in labels.index}
    data = {}
    for now, predictions in zip(nows, result):
        for journey_id, t in predictions.items():
            data.setdefault(journey_id, []).append((now, t))
    for journey_id, points in data.items():
        start_time = points[0][0]
        try:
            end_time = labels_dict[journey_id]
        except KeyError:
            continue
        if end_time <= now:
            plt.plot(*zip(*points))
            plt.plot([start_time, end_time], [end_time, end_time], 'k')
    plt.show()


def predict_bus_times_multi(buses, utm32_position, nows, radius_sq=RADIUS_SQ):
    journeys = buses.groupby('JourneyId')
    # print('%s journeys' % len(journeys))
    x, y = utm32_position
    buses_x, buses_y = project(buses)
    dx = x - buses_x
    dy = y - buses_y
    dist_sq = dx ** 2 + dy ** 2
    close = dist_sq < radius_sq
    buses_close = buses[close]
    journey_close_idx = buses_close.groupby('JourneyId').request_time.idxmin()
    journey_close = buses.loc[journey_close_idx].set_index('JourneyId')
    # journey_close_time = journey_close.request_time
    # close = buses_dist_sq.dist_sq < radius_sq
    # journey_dist_sq = buses_dist_sq.groupby('JourneyId').dist_sq
    result = []
    for now in nows:
        journey_close_now = journey_close[journey_close.request_time <= now]
        close_journey_ids = journey_close_now.index
        most_recent_journey = journey_close_now.request_time.idxmax()
        most_recent_point = journey_close.loc[most_recent_journey]

        most_recent = buses[buses.JourneyId == most_recent_journey]
        most_recent_x, most_recent_y = project(most_recent)

        current_journeys = buses[buses.request_time <= now].groupby('JourneyId')
        far_journey_ids = sorted(current_journeys.groups.keys() -
                                 set(close_journey_ids))
        far_journeys_idx = current_journeys.request_time.idxmax().loc[far_journey_ids]
        far_journeys_point = buses.loc[far_journeys_idx].set_index('JourneyId')
        far_journeys_x, far_journeys_y = project(far_journeys_point)
        # print(far_journeys_point)

        result_now = {}
        recent_threshold = now - datetime.timedelta(seconds=60)
        for journey_id in far_journey_ids:
            t = far_journeys_point.loc[journey_id].Updated
            if t < recent_threshold:
                # print('Skip %s at time %s' % (journey_id, t))
                continue
            dx = far_journeys_x[journey_id] - most_recent_x
            dy = far_journeys_y[journey_id] - most_recent_y
            dist_sq = dx ** 2 + dy ** 2
            closest = dist_sq.idxmin()
            closest_time = buses.loc[closest].Updated
            t_diff = t - closest_time
            predicted = most_recent_point.Updated + t_diff
            # until = predicted - now
            # print('%s is behind by %s, expected in %s at %s' %
            #       (journey_id, t_diff, until, predicted))
            result_now[journey_id] = predicted
            # journey = buses[buses.JourneyId == journey_id]
            # show_buses(journey, '')
            # plt.plot([far_journeys_x[journey_id], most_recent_x[closest]],
            #          [far_journeys_y[journey_id], most_recent_y[closest]], 'x-b')
        result.append(result_now)
    return result, journey_close.Updated


def main():
    with pd.HDFStore('midtbustrack.h5') as store:
        # show_all_buses(store)
        # p = (-505636.59, 56.4725)
        buses = get_buses(store)
        p = (-505636.588946, 56.470429)  # Storcenter Nord
        # find_most_recent_bus(buses, p, datetime.datetime.now() - datetime.timedelta(hours=1))
        plot_prediction(buses, p, datetime.datetime.now())


if __name__ == '__main__':
    main()
