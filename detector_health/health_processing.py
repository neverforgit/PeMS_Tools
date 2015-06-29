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
    Args:
        start_date (date) = start date of the data
        end_date (date) = end date of data 
        delta (int) = how many days to increment by
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
    Args:
        dir (str) = path to directory of data files
        day (str) = name of file, format is YYYY_MM_DD_*
        df (DataFrame) = data frame of health summary values
    Reads a single day of the Detector Health Detail report. Updates the matching column
    in the data frame with the "average lane health" for that day. The "average lane health"
    is the mean of lane statuses for a single station where status = 1 if good and 0 o.w.
    """
    name = day.split('_health')[0]
    #  Create a dataframe for the single day. The 'Status' column describes the percent of lanes that were good for that day
    temp = pd.read_csv(dir+day, sep='\t').groupby('VDS').agg({'Status': lambda x: float(np.sum([xx==0 for i,xx in enumerate(x)]))/float(len(x))})
    #  Match the values in the temp dataframe to the appropriate column in df
    temp.columns = [name] #  rename the column for matching
    #return pd.concat([df, temp])
    df.loc[:, name] = temp.iloc[:,0]
    
def join_files(data_path, out_path='./'):
    """
    Joins all the Detector Health Detail files in the directory into one big csv and dataframe.
    Args:
        dir (str) = path to data directory
        out_path (str) = path to the directory where you want to save the csv
    """
    fnames = os.listdir(data_path)
    #  Count the rows
    r = 0
    start_path = os.getcwd()
    os.chdir(data_path) #  change cwd to data dir
    for name in fnames:
        with open(name, 'r') as f:
            f.next() #  skip header
            for line in f: r+=1
    #  Iitialize the output df
    temp = pd.read_csv(fnames[0], sep='\t')
    h = list(temp.columns) #  header labels
    df = pd.DataFrame(index=np.arange(0,r), columns=['Date']+h)
    #  Read all the files and combine into one dataframe
    i=0
    #TODO Find a faster way to do this loop.
    for name in fnames:
        date = name.split('_health')[0] #  YYYY_MM_DD
        with open(name,'r') as f:
            f.next() #  skip header
            for line in f:
                df.iloc[i,:] = [date] + line.split('\t')
                i += 1
    os.chdir(start_path) #  change back to original dir
    df.to_csv(out_path+date.split('_')[0]+'_joined_health_detail.csv')
    return df
                
        



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "ERROR: you must provide the path to the data directory as a system arg."
        exit
    else:
        data_path = sys.argv[1]

    fnames = [f for f in os.listdir(data_path) if re.match('[0-9][0-9][0-9][0-9]_', f)]  #  Data file names start with year, 'YYYY_'
    year = int(fnames[0].split('_')[0])
    #  Create a sorted list of days
    days = [d.split('_health')[0] for d in fnames].sort()  #  Each day should match YYYY_MM_DD 
    df = pd.DataFrame(columns=['VDS', 'Year_Avg'] + days)  #  Initiate empty data frame
    #  Get the VDS values
    with open(data_path + fnames[-1], 'r') as f:
        f.next() #  skip header
        vds = list(set([line.split('\t')[2] for line in f])) #  convert to set to strip repeats
        vds.sort()
        
    [process_day(data_path, d, df) for d in fnames] #  Process all the days
    #  

