"""
Test two different methods for parsing a large text file and building a data frame
"""

from utils.timer import Timer
import pandas as pd
import numpy as np

path = 'C:\PeMS_scraper\detector_health\data_health\\2014_01_01_health_detail.txt'

temp = pd.read_csv(path, sep='\t')
h = list(temp.columns)


#  Method 1 - read file line by line
df1 = pd.DataFrame(index=np.arange(0,temp.shape[0]), columns=h)
with Timer() as t:
    i = 0
    with open(path, 'r') as f:
        f.next() #  burn header
        for line in f:
            df1.iloc[i,:] = line.split('\t')
            i+=1
print "Line-by-line time to read and write for 1 loop %s" %t.secs


# Method 2 - use read_csv
df2 = pd.DataFrame(index=np.arange(0,temp.shape[0]), columns=h)
with Timer() as t:
    temp = pd.read_csv(path, sep='\t')
    df2[:] = temp[:]
print "read_csv time for 1 loop %s" %t.secs


#  Method 1 - read file line by line
df1 = pd.DataFrame(index=np.arange(0,temp.shape[0]), columns=h)
with Timer() as t:
    i = 0
    with open(path, 'r') as f:
        f.next()  # burn off header
        for line in f:
            i+=1
print "Line-by-line time to only count lines for 1 loop %s" %t.secs