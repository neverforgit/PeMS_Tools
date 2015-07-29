import numpy as np
from numpy.random import rand
import pandas as pd

from utils.timer import Timer

# Some constants
num_dfs = 10  # Number of random dataframes to generate
n_rows = 2500
n_cols = 40
n_reps = 100  # Number of repetitions for timing

# Generate a list of num_dfs dataframes of random values
df_list = [pd.DataFrame(rand(n_rows*n_cols).reshape((n_rows, n_cols)), columns=np.arange(n_cols)) for i in np.arange(num_dfs)]

##
# Define two methods of growing a large dataframe
##

# Method 1 - append dataframes
def method1():
    out_df1 = pd.DataFrame(columns=np.arange(4))
    for df in df_list:
        out_df1 = out_df1.append(df, ignore_index=True)
    return out_df1

# Method 2 - preallocated empty dataframe size
def method2():
    # Create an empty dataframe that is big enough to hold all the dataframes in df_list
    out_df2 = pd.DataFrame(columns=np.arange(n_cols), index=np.arange(num_dfs*n_rows))
    # Set the dtypes of each column
    for ix, col in enumerate(out_df2.columns):
        out_df2[col] = out_df2[col].astype(df_list[0].dtypes[ix])
    # Fill in the values
    for ix, df in enumerate(df_list):
        out_df2.iloc[ix*n_rows:(ix+1)*n_rows, :] = df.values
    return out_df2

# Method 3 - preallocate dataframe with fake data of appropriate type
def method3():
    # Create fake data array
    data = np.transpose(np.array([np.empty(n_rows*num_dfs, dtype=dt) for dt in df_list[0].dtypes]))
    # Create placeholder dataframe
    out_df3 = pd.DataFrame(data)
    # Fill in the real values
    for ix, df in enumerate(df_list):
        out_df3.iloc[ix*n_rows:(ix+1)*n_rows, :] = df.values
    return out_df3

##
# Time both methods
##

# Time Method 1
times_1 = np.empty(n_reps)
for i in np.arange(n_reps):
    with Timer() as t:
       df1 = method1()
    times_1[i] = t.secs
print 'Total time for %d repetitions of Method 1: %f [sec]' % (n_reps, np.sum(times_1))
print 'Best time: %f' % (np.min(times_1))
print 'Mean time: %f' % (np.mean(times_1))

# Time Method 2
times_2 = np.empty(n_reps)
for i in np.arange(n_reps):
    with Timer() as t:
        df2 = method2()
    times_2[i] = t.secs
print 'Total time for %d repetitions of Method 2: %f [sec]' % (n_reps, np.sum(times_2))
print 'Best time: %f' % (np.min(times_2))
print 'Mean time: %f' % (np.mean(times_2))

# Time Method 3
times_3 = np.empty(n_reps)
for i in np.arange(n_reps):
    with Timer() as t:
        df3 = method3()
    times_3[i] = t.secs
print 'Total time for %d repetitions of Method 3: %f [sec]' % (n_reps, np.sum(times_3))
print 'Best time: %f' % (np.min(times_3))
print 'Mean time: %f' % (np.mean(times_3))