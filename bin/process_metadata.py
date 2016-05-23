import ConfigParser
import sys

import numpy as np
import pandas as pd

import utils.meta

__author__ = 'Andrew A Campbell'

'''
This script is used to process all the metadata files for a range of dates. The joined and filetered
metadata file output serves as the reference for building station time series and analysis.
'''

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'ERROR: need to supply the path to the conifg file'
    config_path = sys.argv[1]
    conf = ConfigParser.ConfigParser()
    conf.read(config_path)
    meta_dir = conf.get('Paths', 'meta_dir')
    filtered_meta_path = conf.get('Paths', 'filtered_meta_path')
    bad_meta_path = conf.get('Paths', 'bad_meta_path')



    ##
    # 1 - Join the metadata files
    ##

    meta_joined = utils.meta.join_meta(meta_dir)


    ##
    # 2 - Extract target sensors based on a shapefile
    ##

    #TODO - We are skipping this section for Smart Bay. Just taking everything in D4.


    ##
    # 3 - Filter out moving IDs
    ##
    meta_filtered = utils.meta.filter_moving_ids(meta_joined)  # good Ids
    meta_bad = utils.meta.get_moving_ids(meta_joined)  # bad, moving IDs
    print("Number of good unique sensors: %d" % np.unique(meta_filtered['ID']).size)
    print("Number of bad unique sensors: %d" % np.unique(meta_bad['ID']).size)

    ##
    # 4 - Write filtered metadata
    ##
    meta_filtered.to_csv(filtered_meta_path)
    meta_bad.to_csv(bad_meta_path)

