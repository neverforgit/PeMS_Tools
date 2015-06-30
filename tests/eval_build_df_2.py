"""
Test two different methods for parsing many large text files and building
them into a single data frame.
"""

from utils.timer import Timer
import pandas as pd
import numpy as np

import os, re

# Get a list of data file names
data_path = 'C:\PeMS_scraper\detector_health\data_health'
fnames = [f for f in os.listdir(data_path) if re.match('[0-9]+_[0-9]+_[0-9]+_', f)]
fnames.sort()
os.chdir(data_path)

#  Method 1 - parse all files ahead of time to get size.

with Timer() as t1:
    # Count the rows and initialize data frame
    with Timer() as t2:
        temp = pd.read_csv(fnames[0], sep='\t')
        h = list(temp.columns)
        i = 0
        for name in fnames:
            with open(name, 'r') as f:
                f.next()  # burn the header
                for line in f:
                    i+=1
        df1 = pd.DataFrame(index=np.arange(0, i), columns=h)
    print 'Time to count lines: %s' %t2.secs
    i = 0
    for name in fnames:
        temp = pd.read_csv(name, sep='\t')
        df1.iloc[i:i+temp.shape[0], :] = temp.values
        i += temp.shape[0]
print "Total time to build pre-initialized data frame: %s" %t1.secs


# Method 2 - use read_csv
with Timer() as t3:
    df2 = pd.read_csv(fnames[0], sep='\t')
    for name in fnames[1:]:
        df2 = pd.concat([df2, pd.read_csv(name, sep='\t')])
print "Total time to build data frame by concat: %s" %t3.secs
