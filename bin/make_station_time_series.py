import ConfigParser
import sys

import utils.station

__author__ = 'Andrew A Campbell'

"""
This script is used to generate the station time
"""

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'ERROR: need to supply the path to the conifg file'
    config_path = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read(config_path)
    # Paths
    meta_path = conf.get('Paths', 'meta_path')
    station_dir = conf.get('Paths', 'station_dir')
    time_series_dir = conf.get('Paths', 'time_series_dir')


    ##
    # 1 - Create unique time series folder and files for each station
    ##
    utils.station.generate_time_series_V2(meta_path, station_dir, time_series_dir, n_chunks=8)