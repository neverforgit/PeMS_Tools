import ConfigParser
import os
import sys
import time
import shutil

import numpy as np

import utils.station

__author__ = 'Andrew A Campbell'
# Generates n-minute rollups of the time series

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print 'erROOAAARRRR!!! Need to pass the path to the station parent file.'
        exit()
    config_path = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read(config_path)
    # station_path is the parent directory all the time series
    station_path = conf.get('Paths', 'station_dir')
    out_name = conf.get('Paths', 'out_name')
    agg_period = int(conf.get('Params', 'agg_period'))

    start_dir = os.getcwd()
    os.chdir(station_path)
    stations = [n for n in os.listdir('.') if n.isdigit()]
    tic0 = time.time()
    for stat in stations:
        tic = time.time()
        os.chdir(stat)  # move to individaul station dir and make the rollup
        #TODO check for missing rows. If present, call timeseries_reindex  --> do this in utils.station.rollup_time_series
        utils.station.rollup_time_series(agg_period, '.', out_name)
        os.chdir(station_path)  # move back up to parent dir
        toc = time.time()
        print 'Time to proces station %s: %d' % (stat, toc - tic)
    print 'Total time to build distributions %d' % (time.time() - tic0)
    os.chdir(start_dir)

