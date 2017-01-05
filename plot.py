import datetime
import functools
import pyproj
import pandas as pd
import matplotlib.pyplot as plt

from fetch import timestamp


RADIUS_SQ = 1e-5


def project(df):
    index = df.index
    sproj = pyproj.Proj('+init=epsg:3857')
    dproj = pyproj.Proj(
        '+proj=utm +zone=32 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
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


def h5ls(filename, depth, path=''):
    if path:
        assert path.startswith('/')
        assert not path.endswith('/')
        arg = filename + path
        prefix = path + '/'
    else:
        arg = filename
        prefix = ''

    p = subprocess.Popen(
        ('h5ls', '-r', arg), stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL, universal_newlines=True)
    with p:
        for line in p.stdout:
            line = prefix + line.split()[0]
            if len(line.split('/')) == depth:
                yield line


Journey = collections.namedtuple(
    'Journey',
    '''trajectory Id JourneyId Name StartStation EndStation StartName EndName
    StartTime EndTime DirectionText'''.split())


def iter_journeys(filename, store, line='2A', end_station=auh):
    path = '/line_{line}/towards_{end_station}'.format(
        line=line, end_station=end_station)
    for k in h5ls(filename, depth=6, path=path):
        yield Journey(trajectory=store[k],
                      **store.get_storer(k).attrs.metadata)


@functools.wraps(iter_journeys)
def get_journeys(*args, **kwargs):
    return list(iter_journeys(*args, **kwargs))


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


def plot_prediction(journeys, utm32_position, now):
    hours = 10
    nows = [now + datetime.timedelta(seconds=s)
            for s in range(-3600*hours, 0, 60)]
    result, labels = predict_bus_times_multi(journeys, utm32_position, nows)
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


def close_filter(bus, utm32_position, radius_sq):
    x, y = utm32_position
    buses_x, buses_y = project(bus)
    dx = x - buses_x
    dy = y - buses_y
    dist_sq = dx ** 2 + dy ** 2
    close = dist_sq < radius_sq
    return close


def predict_bus_times_multi(journeys, utm32_position, nows, radius_sq=RADIUS_SQ):
    TODO journeys instead of buses
    bus_by_journey = {attr['JourneyId']: bus for bus, attr in buses}
    journey_close = {}
    for bus, attr in buses:
        close = bus[close_filter(bus, utm32_position, radius_sq)]
        if not close.empty:
            idx = close.index.min()
            rec = close.loc[idx]
            rec.request_time = idx
            journey_close[attr['JourneyId']] = rec
    journey_close = pd.DataFrame.from_records(journey_close.values(),
                                              index=journey_close.keys())

    result = []
    for now in nows:
        journey_close_now = journey_close[journey_close.request_time <= now]
        close_journey_ids = journey_close_now.index
        most_recent_journey = journey_close_now.request_time.idxmax()
        most_recent_point = journey_close.loc[most_recent_journey]

        most_recent = bus_by_journey[most_recent_journey]
        most_recent_x, most_recent_y = project(most_recent)

        current_buses = buses[buses.index <= now]
        current_journeys = current_buses.groupby('JourneyId')
        far_journey_ids = sorted(current_journeys.groups.keys() -
                                 set(close_journey_ids))
        far_journeys_idx = (
            current_journeys.index.idxmax().loc[far_journey_ids])
        far_journeys_point = buses.loc[far_journeys_idx].set_index('JourneyId')
        far_journeys_x, far_journeys_y = project(far_journeys_point)

        result_now = {}
        recent_threshold = now - datetime.timedelta(seconds=60)
        for journey_id in far_journey_ids:
            t = far_journeys_point.loc[journey_id].Updated
            if t < recent_threshold:
                continue
            dx = far_journeys_x[journey_id] - most_recent_x
            dy = far_journeys_y[journey_id] - most_recent_y
            dist_sq = dx ** 2 + dy ** 2
            closest = dist_sq.idxmin()
            closest_time = buses.loc[closest].Updated
            t_diff = t - closest_time
            predicted = most_recent_point.Updated + t_diff
            result_now[journey_id] = predicted
        result.append(result_now)
    return result, journey_close.Updated


def main():
    filename = 'midtbustrack.h5'
    with pd.HDFStore(filename) as store:
        # show_all_buses(store)
        # p = (-505636.59, 56.4725)
        journeys = get_journeys(filename, store)
        p = (-505636.588946, 56.470429)  # Storcenter Nord
        # find_most_recent_bus(
        #     buses, p, datetime.datetime.now() - datetime.timedelta(hours=1))
        plot_prediction(journeys, p, datetime.datetime.now())


if __name__ == '__main__':
    main()
