import datetime
import functools
import subprocess
import collections

import pyproj
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from fetch import timestamp, parse_bus_key


RADIUS_SQ = 1e-5


def project(df):
    try:
        return df.x, df.y
    except AttributeError:
        pass
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
        prefix = path
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


JourneyBase = collections.namedtuple(
    'Journey',
    '''trajectory Id JourneyId Name StartStation EndStation StartName EndName
    StartTime EndTime DirectionText'''.split())


class Journey(JourneyBase):
    def attrib(self):
        return {k: getattr(self, k) for k in self._fields
                if k != 'trajectory'}

    def until(self, now):
        t = self.trajectory
        t_until = t[t.index <= now]
        return type(self)(trajectory=t_until, **self.attrib())

    def last_seen(self):
        return self.trajectory.index.max()

    def last_record(self):
        return self.trajectory.loc[self.last_seen()]

    def last_updated(self):
        return self.last_record().Updated

    @property
    def empty(self):
        return self.trajectory.empty


def iter_journeys(filename, store, line='2A', end_station=auh, date=None):
    if date is None:
        date = datetime.date.today()
    date_str = date.strftime('%Y_%m_%d')
    path = '/line_{line}/towards_{end_station}/date_{date}'.format(
        line=line, end_station=end_station, date=date_str)
    for k in h5ls(filename, depth=6, path=path):
        try:
            trajectory = store[k]
        except:
            print(k)
            raise
        trajectory['x'], trajectory['y'] = project(trajectory)
        o = parse_bus_key(k)
        yield Journey(trajectory=trajectory, JourneyId=o['journey'],
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


def print_stats(stats):
    df = pd.DataFrame.from_records(stats)
    print(df)


def plot_prediction(journeys, utm32_position, now):
    hours = 10
    nows = [now + datetime.timedelta(seconds=s)
            for s in range(-3600*hours, 0, 60)]
    result, labels = predict_bus_times_multi(journeys, utm32_position, nows)
    data = {}
    for now, predictions in zip(nows, result):
        for journey_id, t in predictions.items():
            data.setdefault(journey_id, []).append((now, t))
    stats = []
    for journey_id, points in data.items():
        start_time = points[0][0]
        try:
            end_time = labels[journey_id]
        except KeyError:
            continue
        if end_time <= now:
            plt.plot(*zip(*points))
            plt.plot([start_time, end_time], [end_time, end_time], 'k')
        start_stats = end_time - datetime.timedelta(minutes=5)
        relative = [(t - end_time).total_seconds() for n, t in points
                    if n >= start_stats]
        min, max, mean = np.min(relative), np.max(relative), np.mean(relative)
        stats.append(dict(t=end_time, min=min, max=max, mean=mean))
    print_stats(stats)
    plt.show()


def close_filter(bus, utm32_position, radius_sq):
    x, y = utm32_position
    buses_x, buses_y = project(bus)
    dx = x - buses_x
    dy = y - buses_y
    dist_sq = dx ** 2 + dy ** 2
    close = dist_sq < radius_sq
    return close


def align_trajectory(targets, journey):
    rec = journey.last_record()
    res = []
    for t in targets:
        dx = t.trajectory.x - rec.x
        dy = t.trajectory.y - rec.y
        d_sq = dx ** 2 + dy ** 2
        closest = d_sq.idxmin()
        res.append(t.trajectory.Updated.loc[closest])
    return res


def median(xs):
    return sorted(xs)[len(xs) // 2]


def predict_bus_times_multi(journeys, utm32_position, nows, radius_sq=RADIUS_SQ):
    assert all(isinstance(j, Journey) for j in journeys)
    # journeys_dict = {j.JourneyId: j for j in journeys}
    journey_close = {}
    journey_close_time = {}
    for journey in journeys:
        f = close_filter(journey.trajectory, utm32_position, radius_sq)
        sub_trajectory = journey.trajectory[f]
        if not sub_trajectory.empty:
            idx = sub_trajectory.index.min()
            rec = sub_trajectory.loc[idx]
            journey_close_time[journey.JourneyId] = idx
            journey_close[journey.JourneyId] = rec
    labels = {journey_id: rec.Updated
              for journey_id, rec in journey_close.items()}

    result = []
    for now in nows:
        print(now)
        close_journey_times_now = {
            j: t for j, t in journey_close_time.items() if t <= now}
        close_journeys_now = sorted(
            (j for j in journeys if j.JourneyId in close_journey_times_now),
            key=lambda j: close_journey_times_now[j.JourneyId],
            reverse=True)
        upcoming_journeys = []
        recent_threshold = now - datetime.timedelta(seconds=60)
        result_now = {}
        for journey in journeys:
            if journey.JourneyId in close_journey_times_now.keys():
                # This journey is used as benchmark
                continue
            journey = journey.until(now)
            if journey.empty:
                # No trajectory data
                continue
            if journey.last_seen() < recent_threshold:
                # No current data
                continue
            upcoming_journeys.append(journey)
            n = 5
            alignment = align_trajectory(
                close_journeys_now[:5], journey)
            journey_updated = journey.last_updated()
            predictions = [
                journey_updated + (labels[j.JourneyId] - a)
                for j, a in zip(close_journeys_now, alignment)]
            result_now[journey.JourneyId] = median(predictions[:n])
        result.append(result_now)
    return result, labels


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
