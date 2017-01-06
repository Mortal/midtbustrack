import os
import argparse
import datetime
import subprocess
import pandas as pd
from fetch import parse_bus_key


def h5ls(filename, depth):
    p = subprocess.Popen(
        ('h5ls', '-r', filename), stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL, universal_newlines=True)
    with p:
        for line in p.stdout:
            line = line.split()[0]
            if len(line.split('/')) == depth:
                yield line


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-k', '--key')
    parser.add_argument('-n', '--skip-repack', action='store_true')
    args = parser.parse_args()

    filename = 'midtbustrack.h5'
    initial_size = os.stat('midtbustrack.h5').st_size
    with pd.HDFStore(filename) as store:
        if args.key:
            keys = [args.key]
        else:
            keys = []
            today = datetime.date.today()
            for k in h5ls(filename, 6):
                o = parse_bus_key(k)
                date = datetime.datetime.strptime(o['date'], '%Y_%m_%d')
                date = date.date()
                if date < today:
                    keys.append(k)
                continue
        for k in keys:
            storer = store.get_storer(k)
            if not storer:
                raise ValueError(k)
            if storer.format_type == 'table':
                df = storer.read()
                del storer
                del store[k]
                store.put(k, df, format='fixed')
            elif storer.format_type == 'fixed':
                # print("%s is fixed" % k)
                pass
            else:
                raise ValueError(storer.format_type)
    if not args.skip_repack:
        tmp = 'repacked.h5'
        subprocess.check_call(('h5repack', filename, tmp))
        os.rename(tmp, filename)
    result_size = os.stat('midtbustrack.h5').st_size
    print("Initial size:   %11d" % initial_size)
    print("Resulting size: %11d" % result_size)


if __name__ == '__main__':
    main()
