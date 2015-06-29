from timer import Timer
import pandas as pd
import numpy as np

path = '/Users/daddy30000/14_Mobility_Sim/GoogleDrive/SFCTA/PeMS/data/health_detail/2014_1_1_health_detail.txt'

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
print "Line-by-line time for 1 loop %s" %t.secs


# Method 2 - use read_csv
df2 = pd.DataFrame(index=np.arange(0,temp.shape[0]), columns=h)
with Timer() as t:
    temp = pd.read_csv(path, sep='\t')
    df2[:] = temp[:]
print "read_csv time for 1 loop %s" %t.secs
