__author__ = 'Andrew Campbell'
"""
This script examines meta files over a range of dates. It identifies changes in meta information. This is useful
for indentifying:
1) the addition/removal of stations and the subsequent changes of miles assigned to a station.
2) inconsistencies (i.e. mistakes) in the mapping of VDS ID to location
"""

import sys, os
import pandas as pd
import numpy as np
from ConfigParser import ConfigParser
from numpy.linalg import norm

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "ERROR: need to provide path to config file."
        exit
    config_path = sys.argv[1]

    # Load config and get list of file names to read
    config = ConfigParser()
    config.read(config_path)
    meta_path = config.get('Paths', 'meta_dir_path')
    out_path = config.get('Paths', 'out_dir_path')
    fnames = [n for n in os.listdir(meta_path) if n[0:13] == 'd04_text_meta']
    fnames.sort()
    start_dir = os.getcwd()
    os.chdir(meta_path)

    ###############################################################################
    #  Version 1 - look for any changes in text
    ###############################################################################
    # NOTE: This did not work very well. Roughly half the entries have slight differences

    # Read each meta file and create a data frame of unique values
    # and another frame listing only VDS stations with changes
    ##

    # Seed the df of unique values
    df = pd.read_csv(fnames[0], sep='\t')
    dfd = df.copy()  # copy with a Date column added
    # We keep the original df without dates to avoid having to recreate it everytime we
    # we check if it contains a row in the loop below.
    d = fnames[0].split('.')[0][-10:]
    dfd['Date'] = d

    for name in fnames[1:]:
        print 'Checking ' + name
        temp = pd.read_csv(name, sep='\t')
        d = name.split('.')[0][-10:]
        for i, r in temp.iterrows():
            # if True not in (df == r).all(axis=1): # row is new
            if not (df == r).all(axis=1).any(): # row is new
                df.loc[df.index[-1]+1] = r
                # df.append(r)
                r['Date'] = d
                dfd.loc[dfd.index[-1]+1] = r
    dfd = dfd.sort('ID')
    dfd.to_csv(out_path+'uniqe_rows.csv', sep='\t')
    os.chdir(start_dir)

    ###############################################################################
    #  Version 2 - look for significant deviations in x,y
    ###############################################################################
    #
    #This version is much nicer than V1

    os.chdir(meta_path)  # work where the data are
    df = pd.read_csv(fnames[0], sep='\t', index_col='ID')[['Latitude', 'Longitude']]
    df['Date'] = fnames[0].split('.')[0][-10:]
    thresh = 0.001  # threshold for x,y displacement in degrees (about 100 m)
    out = pd.DataFrame(columns=['ID','Latitude', 'Longitude', 'Date'])
    i = 0
    for name in fnames[1:]:
        print 'Checking ' + name
        temp = pd.read_csv(name, sep='\t', index_col='ID')[['Latitude', 'Longitude']]
        temp['Date'] = name.split('.')[0][-10:]
        for idx in temp.index:
            if idx not in df.index:
                df.loc[idx] = temp.loc[idx]
            elif norm(df.loc[idx,['Latitude', 'Longitude']] - temp.loc[idx,['Latitude', 'Longitude']]) > thresh:
                out.loc[i] = df.loc[idx]
                out.loc[i, 'ID'] = idx
                i += 1
                out.loc[i] = temp.loc[idx]
                out.loc[i, 'ID'] = idx
                i += 1
    out.to_csv(out_path+'moving_IDs.csv', sep='\t')
