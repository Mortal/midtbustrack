import re
import time
import datetime
import pandas as pd
import requests
from xml.etree import ElementTree as ET


INTERVAL = 15
USER_AGENT = 'midtbustrack/0.1 (https://github.com/Mortal/midtbustrack)'
LAT = '56.154437121004236'
LON = '10.204795170878484'
RADIUS = '11379.443078698932'
DOMAIN = 'https://live.midttrafik.dk'
URL = DOMAIN + '/getbuses.php?lat={lat}&lon={lon}&radius={radius}'

COLUMNS = '''
    Id Name Updated Delay Lat Lon JourneyId Distance Line StartStation
    EndStation StartName EndName StartTime EndTime DirectionText
'''.split()


def is_connected(session):
    try:
        session.get(DOMAIN, allow_redirects=False)
    except requests.ConnectionError:
        return False
    else:
        return True


def network_wait(session):
    connected = is_connected(session)
    if connected:
        return

    try:
        import pyroute2
    except ImportError:
        raise Exception('pyroute2 not installed')

    ip = pyroute2.IPRSocket()
    ip.bind()
    print('Cannot connect to %s - wait for network to come up' % DOMAIN,
          flush=True)
    try:
        while not connected:
            changed = False
            while not changed:
                for o in ip.get():
                    if o['event'] == 'RTM_NEWROUTE':
                        changed = True
            # Check if we are now connected
            connected = is_connected(session)
            if not connected:
                print('Got a new route, but still not connected',
                      flush=True)
    finally:
        ip.close()
    print('Got network connection',
          flush=True)


def get_buses_xml(session):
    url = URL.format(lat=LAT, lon=LON, radius=RADIUS)
    response = session.get(url)
    return ET.fromstring(response.content)


def timestamp(input_str):
    return datetime.datetime.strptime(input_str, '%Y-%m-%d %H:%M:%S')


def parse_bus(bus_element):
    assert bus_element.tag == 'Bus'
    FIELDS = dict(
        Id=int,
        Name=str,
        Updated=timestamp,
        Delay=int,
        Lat=float,
        Lon=float,
        JourneyId=str,  # GUID (fixed length)
        Distance=int,
        Line=str,  # Fixed length (at most 3)
        StartStation=int,
        EndStation=int,
        StartName=str,
        EndName=str,
        StartTime=timestamp,
        EndTime=timestamp,
        DirectionText=str,
    )
    attrib = dict(bus_element.items())
    assert attrib.keys() == FIELDS.keys()
    return {k: FIELDS[k](v) for k, v in attrib.items()}


def append_rec(store, index, key, rec, attr):
    store.append(key, pd.DataFrame.from_records([rec], index=[index]))
    store.get_storer(key).attrs.metadata = attr


def slugify(s):
    return re.sub(r'[^A-Za-z0-9]+', '_', str(s))


def bus_key(bus):
    line = slugify(bus['Line'])
    journey = slugify(bus['JourneyId'])
    id = slugify(bus['Id'])
    fmt = ('/line_{line}/towards_{endstation}/date_{date}/' +
           'id_{id}/journey_{journey}')
    return fmt.format(
        line=line, endstation=bus['EndStation'],
        date=bus['StartTime'].strftime('%Y_%m_%d'),
        id=id, journey=journey)


def parse_bus_key(key):
    fmt = ('/line_{line}/towards_{endstation}/date_{date}/' +
           'id_{id}/journey_{journey}')
    pattern = re.sub(r'\{([a-z]+)\}', r'(?P<\1>[A-Za-z0-9_]+)', fmt)
    mo = re.match('^%s$' % pattern, key)
    if not mo:
        raise ValueError(key)
    return mo.groupdict()


def append_bus_location(store, bus, request_time):
    key = bus_key(bus)
    attr_keys = '''
    Id Name StartStation EndStation StartName EndName StartTime EndTime
    DirectionText'''.split()
    attr = {k: bus[k] for k in attr_keys}
    rec_keys = 'Updated Delay Lat Lon'.split()
    rec = {k: bus[k] for k in rec_keys}
    append_rec(store, request_time, key, rec, attr)


def parse_buses(xml):
    assert xml.tag == 'Result'
    return [parse_bus(bus) for bus in xml]


def append_buses(store, session):
    session.headers = {'User-Agent': USER_AGENT}
    t1 = time.time()
    xml = get_buses_xml(session)
    t2 = time.time()
    buses = parse_buses(xml)
    t3 = time.time()
    request_time = datetime.datetime.now()
    for bus in buses:
        append_bus_location(store, bus, request_time)
    t4 = time.time()
    return ('HTTP-GET:%4.2f XML-parse:%4.2f append:%4.2f' %
            (t2-t1, t3-t2, t4-t3))


def main():
    store = pd.HDFStore('midtbustrack.h5')
    session = requests.Session()
    with store, session:
        network_wait(session)
        while True:
            times = append_buses(store, session)
            t1 = time.time()
            store.flush()
            t2 = time.time()
            sleep = INTERVAL - (time.time() % INTERVAL)
            t = datetime.datetime.now() + datetime.timedelta(seconds=sleep)
            print('%s flush:%4.2f sleep:%5.2f until %s' %
                  (times, t2 - t1, sleep, t.replace(microsecond=0)),
                  flush=True)
            time.sleep(sleep)


if __name__ == '__main__':
    main()
