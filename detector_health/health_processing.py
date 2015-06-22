__author__ = "Andrew A. Campbell"
__email__ = "andrew dot campbell at sfcta dot org"
"""
Script will process a directory of daily detector health reports. Note that this
assumes a daily report frequency! The output is a Pandas data frame where 
each column is the average health, accross all lanes (h = 1 if good, h = 0 o.w)
as well as the yearly average.
"""

import os, sys

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
    Args:
        dir (str) = path to directory of data files
        day (str) = name of file, format is YYYY_M_D_*
        df (DataFrame) = data frame of health summary values
    Reads a single day of the Detector Health Detail report. Updates the matching column
    in the data frame with the average lane health for that day.
    """
    name = day.split('_health')[0]
    temp = pd.read_csv(dir+day, sep='\t')
    
    """"TODO do a groupby on the VDS id
    use the agg(...) method to take the mean of the health, where y_i = 1 if good and = 0 o.w.
    e.g.
    grouped.agg({'C' : np.sum,
   ....:              'D' : lambda x: np.std(x, ddof=1)})
    
    """
    



if __name__ = "main":
    if len(sys.argv) < 2:
        print "ERROR: you must provide the path to the data directory as a system arg."
        exit
    else:
        data_path = sys.argv[1]
    
    fnames = os.listdir(data_path) #  list of data files to process
    year = int(fnames[0].split('_')[0])
    #days = daterange(date(year,1,1), date(year,12,31)) #  List of all days in year 
    days = [d.split('_health')[0] for d in fnames]
    df = pd.DataFrame(columns=['VDS', 'Year_Avg'] + days)  #  Initiate empty data frame
        
    [process_day(d, df) for d in fnames] #  Process all the days
    