__author__ = "Andrew A. Campbell"
__email__ = "andrew dot campbell at sfcta dot org"
"""
Script will process a directory of daily detector health reports. Note that this
assumes a daily report frequency! The output is a Pandas data frame where
each column is the average health, accross all lanes (h = 1 if good, h = 0 o.w)
as well as the yearly average.
"""

import os, sys, re

import pandas as pd
import numpy as np

from datetime import date, timedelta


def daterange(start_date, end_date, delta=1):
    """
    Tool for iterating over range of dates.
    Returns a list of the starting date of each new query.
    """
    out = []
    days = (end_date - start_date).days
    for n in range(days/delta+1):
        out.append(start_date + timedelta(n*delta))
    return out

def process_day(dir, day, df):
    """
   Reads a single day of the Detector Health Detail report. Updates the matching column in the data frame with the average lane health for that day.

    Args:
        dir (str) = path to directory of data files
        day (str) = name of file, format is YYYY_M_D_*
        df (DataFrame) = data frame of health summary values

    """
    name = day.split('_health')[0]
    #  Create a dataframe for the single day. The 'Status' column describes the percent of lanes that were good for that day
    temp = pd.read_csv(dir+day, sep='\t').groupby('VDS').agg({'Status': lambda x: float(np.sum([xx==0 for i,xx in enumerate(x)]))/float(len(x))})
    #  Match the values in the temp dataframe to the appropriate column in df
    temp.columns = [name] #  rename the column for matching
    return pd.concat([df, temp])

def join_all(data_path, out_path):
    """
    Reads all the daily dectector Health Detail csvs and joins them into one big
    dataframe. Writes the joined df to a csv and returns the df.

    Args:
        data_path (str) = path to directory with data files
        out_path (str) = path to directory where joined csv is written
    """
    fnames = [f for f in os.listdir(data_path) if re.match('[0-9]+_[0-9]+_[0-9]+_', f)]
    start_path = os.getcwd()
    os.chdir(data_path)
    ##
    #  Seed the output dataframe
    ##
    temp = pd.read_csv(fnames[0], sep='\t')
    h = list(temp.columns)
    #  Count the number of lines so we initialize the dataframe with proper memory allocation
    i = 0
    for name in fnames:
        with open(name, 'r') as f:
            f.next() #  Burn the header
            for line in f:
                i+=1
    df = pd.DataFrame(index=np.arange(0,i), columns=['Date']+h)
    ##
    #  Read each file into a temporary data frame and add to the output df
    ##
    i = 0
    for name in fnames:
        day = name.split('_health')[0]
        temp = pd.read_csv(name, sep='\t')
        df.iloc[i:i+temp.shape[0],0] = day
        df.iloc[i:i+temp.shape[0],1:] = temp[:]
        i += temp.shape[0]
    ##
    #  Write the output
    ##
    os.chdir(start_path)
    year = fnames[0].split('_')[0]
    df.to_csv(start_path + '/_' + year + '_joined_health_detail.txt', sep='\t')
    return df


