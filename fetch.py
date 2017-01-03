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
    # These fields have variable-length names
    SKIP_FIELDS = {'Name', 'StartName', 'EndName', 'DirectionText'}
    attrib = dict(bus_element.items())
    assert attrib.keys() == FIELDS.keys() | SKIP_FIELDS
    return {k: FIELDS[k]() if k in SKIP_FIELDS else FIELDS[k](v)
            for k, v in attrib.items()}


def parse_buses(xml):
    assert xml.tag == 'Result'
    return pd.DataFrame.from_records(
        [parse_bus(bus) for bus in xml], columns=COLUMNS)


def append_buses(store, session):
    session.headers = {'User-Agent': USER_AGENT}
    buses = parse_buses(get_buses_xml(session))
    buses['request_time'] = datetime.datetime.now()
    store.append('buses', buses)


def main():
    store = pd.HDFStore('midtbustrack.h5')
    session = requests.Session()
    with store, session:
        while True:
            append_buses(store, session)
            store.flush()
            sleep = INTERVAL - (time.time() % INTERVAL)
            time.sleep(sleep)


if __name__ == '__main__':
    main()
