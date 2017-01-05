import time
import datetime
from xml.etree import ElementTree as ET
from fetch import parse_bus_key
import pandas as pd


def main():
    filename = 'output.h5'
    print("Open %s..." % filename)
    t0 = time.time()
    with pd.HDFStore(filename) as store:
        t1 = time.time()
        print("Opened %s. Get keys..." % (t1 - t0))
        keys = list(store.keys())
        t2 = time.time()
        print("%s keys %s" % (len(keys), (t2 - t1)))
        for k in keys:
            print(k)
            try:
                key = parse_bus_key(k)
            except ValueError:
                print('Unrecognized: %s' % k)
                continue
            storer = store.get_storer(k)
            attr = storer.attrs.metadata
            print(key)
            print(attr)
            print(store[k])


if __name__ == '__main__':
    main()
