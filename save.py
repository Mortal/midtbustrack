'''One-off program to experiment with file format'''
import datetime
from xml.etree import ElementTree as ET
from fetch import parse_bus, append_bus_location
import pandas as pd


def main():
    with open('getbuses.xml') as fp:
        t1 = ET.fromstring(fp.read())
    with open('getbuses2.xml') as fp:
        t2 = ET.fromstring(fp.read())
    assert t1.tag == t2.tag == 'Result'
    buses1 = [parse_bus(bus) for bus in t1]
    buses2 = [parse_bus(bus) for bus in t2]
    time2 = datetime.datetime.now() - datetime.timedelta(seconds=5)
    time1 = time2 - datetime.timedelta(seconds=5)
    for buses, t in [(buses1, time1), (buses2, time2)]:
        with pd.HDFStore('test.h5') as store:
            for bus in buses:
                append_bus_location(store, bus, t)


if __name__ == '__main__':
    main()
