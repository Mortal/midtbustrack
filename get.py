from collections import Counter

import pandas as pd


def show_buses(store):
    buses = store['buses']
    where_2A = buses['Line'] == '2A'
    print(Counter(buses[where_2A]['Id']))


def main():
    with pd.HDFStore('midtbustrack.h5') as store:
        show_buses(store)


if __name__ == '__main__':
    main()
