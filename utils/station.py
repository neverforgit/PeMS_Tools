import datetime
import gc
import os
import shutil
import sys
import time

import numpy as np
import pandas as pd

import utils.util_exceptions

__author__ = 'Andrew Campbell'

"""
These tools are used for processing Station 5-Minute raw data files. These files are reported at the district  level of
spatial aggregation, which may be too big or small for the level of analysis. These tools allow you to extract the rows
that fall within a shapefile polygon that defines your casestudy area.
"""

######################################################################################################################
# Worker functions
######################################################################################################################
# These are the functions that do all the heavy lifting

def get_station_targets(station_path, meta_path, out_path,
                        preamble='d04_text_station', prologue='_FCMS_extract.txt'):
    """
    Extracts station data rows from only the stations that have and ID that appears in the metadata file defined
    by meta_path. The meta_path file should be the aggregate of all stations we want to study. Thus, all station
    filtering should be done in the creation of this aggregate metadata file. Writes the target stations to
    csv files in the same format.
    :param station_path: (str) Path to directory with station data.
    :param meta_path: (str) Path to a file with the meta data for all case study stations. This should be the text file
    written by get_meta_targets()
    :param out_path: (str) Path to directory to write output files
    :param preamble: (str) The leading characters of the name of station data files
    :param prologue: (str) The characters to append to the ened of extracted data files.
    :return: (None)
    """
    start_dir = os.getcwd()
    os.chdir(station_path)
    fnames = [n for n in os.listdir('.') if n[0:len(preamble)] == preamble]  # List of all file name to read
    target_ids = np.unique(pd.read_csv(meta_path, sep='\t')['ID'])  # IDs of stations in case study area

    # Process every file in fnames
    row_count = 0
    for name in fnames:
        print 'Processing: ' + name
        temp = pd.read_csv(name, sep=',', compression='gzip', header=None)
        temp = temp[temp.apply(lambda x: x[1] in target_ids, axis=1)]  # Keep only rows that are in the metadata file
        row_count += temp.shape[0]
        temp.to_csv(out_path+name.split('.')[0]+prologue, sep=',', index=False, header=False)
    print 'Total extracted observations: %d' % row_count
    os.chdir(start_dir)

def station_files_to_df(station_path, preamble='d04_text_station', concat_intv=10):
    """
    Reads all the individual station files in directory at station_path and returns them as a single dataframe.
    :param station_path: (str) Path to directory with station data files.
    :param concat_intv: (int) Aggregation interval is the number of files to open and convert to data frame before
    aggregating. It would be fastest to open everything and concat only once. But this could cause memory problems.
    :param preamble: (str) Text that target file names begin with.
    :return: (pd.DataFrame) Dataframe containing all the data from the individual files with a date column appended.
    WARNING: This method only keeps the totals for each station. The lane-level data are thrown away.
    """
    head = ['Timestamp', 'Station', 'District', 'Fwy', 'Dir', 'Type',
            'Length', 'Samples', 'Observed', 'Total_Flow', 'Avg_Occ', 'Avg_Speed'] # Header for output df
    df = pd.DataFrame(columns=head)
    start_dir = os.getcwd()
    os.chdir(station_path)
    fnames = [n for n in os.listdir('.') if n[0:len(preamble)] == preamble]  # List of all file name to read
    temp_list = [df]
    for name in fnames:
        print 'Adding file: ' + name
        temp = pd.read_csv(name, sep=',', header=None).iloc[:, 0:len(head)]
        temp.columns = head
        temp_list.append(temp)
        #TODO cast the Station column to int
        if len(temp_list) == concat_intv:
            temp_list = [pd.concat(temp_list)]
    os.chdir(start_dir)
    return pd.concat(temp_list)

def join_stations(station_path, out_path,  preamble='d04_text_station'):
    """
    Combines all the station files into one long text file.
    :param station_path: (str) Path to directory with station data files
    :param out_path: (str) Path to directory to write the combined file.
    :param preamble: (str) Text that target file names begin with.
    :return: (None)
    """
    start_dir = os.getcwd()
    os.chdir(station_path)
    fnames = [n for n in os.listdir('.') if n[0:len(preamble)] == preamble]  # List of all file name to read
    fnames.sort()  # Sorting names ensures the dates will be read chronologically
    with open(fnames[0], 'r') as f:  # Get the earliest date
        start_date = f.next().split(',')[0].split()[0].split('/')  # [dd, mm, yyyy]
    with open(fnames[-1], 'r') as f:  # Get the latest date
        for line in f:
            pass
        last_date = line.split(',')[0].split()[0].split('/')  # [dd, mm, yyyy]
    out_name = preamble + '_combined_' + start_date[1] + '_' + start_date[2] + '_' + last_date[1] + '_' + \
               last_date[2] + '.txt'
    with open(out_path+out_name, 'w') as fo:  # Combine all the files
        for name in fnames:
            with open(name, 'r') as fi:
                for line in fi:
                    fo.writelines(line)
    os.chdir(start_dir)

def generate_time_series(meta_target_path, station_path, out_path, preamble='d04_text_station_5min', n_chunks=4):
    """
    Creates individual time series of counts and speeds for each station ID in the aggregated metadata file at
    meta_target_path. A sub directory is created for each unique ID. The time series for that ID is saved as a
    csv in that sub directory. Also write a small csv with aggregated measures: range of dates observed, number of
    observations, standard deviation of link length.

    This function if constrained by a trade-off between memory and processing time. The n_chunks lets you control this
    trade-off. Increasing n_chunks decreases the memory overhead but increases the processing time.

    :param meta_target_path: (str) Path to the aggregated metadata file of target stations. This file is the canonical
    set of station IDs to use in the study!
    :param station_path: (str) Path to the directory containing all the extracted raw station data files to be
    processed. This directory should have been created by utils.station.get_station_targets().
    :param out_path: (str) Path to the parent directory for the output time series.
    :param preamble: (str) The leading characters of station data file names. Prevents trying to parse hidden files etc.
    :param n_chunks: (int) Number of chunks to break the set of unique IDs into.
    :return: (None)
    """
    # Step 0 - Define constants and get list of file names to open and read.
    head = ['Timestamp', 'Station', 'District', 'Fwy', 'Dir', 'Type',
            'Length', 'Samples', 'Observed', 'Total_Flow', 'Avg_Occ', 'Avg_Speed']  # Header for output df
    fnames = [n for n in os.listdir(station_path) if n[0:len(preamble)] == preamble]
    fnames.sort()  # Sort names in ascending chronological order
    start_dir = os.getcwd()
    # Step 1 - Get all unique station IDs from meta_target_path. Aggregated into n_chunks arrays
    target_ids = np.unique(pd.read_csv(meta_target_path, sep='\t')['ID'])  # IDs of stations in case study area
    n = target_ids.shape[0] / n_chunks  # Number of IDs per set
    chunks = []
    for i in np.arange(n_chunks):
        chunks.append(target_ids[n*i:n*(i+1)])
    chunks[-1] = np.append(chunks[-1], target_ids[n*(i+1):])  # Append the remainder to the last set
    # Step 2 - Iterate through each of the n_chunks of IDs
    for i, chunk in enumerate(chunks):  # Each chunk is a subset of the station IDs to be processed
        print 'Processing chunk number: %d' % i
        # Initiate one large temp dataframe for holding values for all IDs in chunk
        max_rows = 288*365*len(chunk)  # Max number of possible observations for chunk.
        temp_chunk = pd.DataFrame(columns=head, index=np.arange(max_rows))
        temp_chunk.iloc[:, :] = -9
        # Step 3 - Iterate through all the station data files
        os.chdir(station_path)
        ix = 0  # Index of current row in temp_chunk
        for name in fnames:
            tic = time.time()
            print 'Processing ' + name
            temp = pd.read_csv(name, sep=',', index_col=False)
            # Step 4 - Iterate through all the IDs in the chunk, extract the time series and append to temp_chunk.
            for stat_id in chunk:
                temp_ts = get_id_time_series(temp, stat_id)  # Time series for stat_id
                temp_chunk.iloc[ix: ix+temp_ts.shape[0], :] = temp_ts.values
                ix += temp_ts.shape[0]
                print ix
            print 'Time to process %f' % (time.time() - tic)
        # Step 5 - Write the output for IDs in the current chunk
        for stat_id in chunk:
            # Write the time series
            os.chdir(out_path)
            os.mkdir(str(stat_id))
            os.chdir(str(stat_id))
            temp_ts = temp_chunk[temp_chunk['Station'] == stat_id]
            temp_ts.to_csv('time_series.csv', sep=',', index=False)
            ts_agg_measures(temp_ts).to_csv('summary.csv', sep=',', index=False)
    os.chdir(start_dir)

#TODO remove and archive original version. Get rid of references to V2
def generate_time_series_V2(meta_target_path, station_path, out_path, preamble='d04_text_station_5min', n_chunks=4):
    """
    Creates individual time series of counts and speeds for each station ID in the aggregated metadata file at
    meta_target_path. A sub directory is created for each unique ID. The time series for that ID is saved as a
    csv in that sub directory. Also write a small csv with aggregated measures: range of dates observed, number of
    observations, standard deviation of link length.

    This function if constrained by a trade-off between memory and processing time. The n_chunks lets you control this
    trade-off. Increasing n_chunks decreases the memory overhead but increases the processing time.

    :param meta_target_path: (str) Path to the aggregated metadata file of target stations. This file is the canonical
    set of station IDs to use in the study!
    :param station_path: (str) Path to the directory containing all the extracted raw station data files to be
    processed. This directory should have been created by utils.station.get_station_targets().
    :param out_path: (str) Path to the parent directory for the output time series.
    :param preamble: (str) The leading characters of station data file names. Prevents trying to parse hidden files etc.
    :param n_chunks: (int) Number of chunks to break the set of unique IDs into.
    :return: (None)
    """
    # Step 0 - Define constants and get list of file names to open and read.
    head = ['Timestamp', 'Station', 'District', 'Fwy', 'Dir', 'Type',
            'Length', 'Samples', 'Observed', 'Total_Flow', 'Avg_Occ', 'Avg_Speed']  # Header for output df
    fnames = [n for n in os.listdir(station_path) if n[0:len(preamble)] == preamble]
    fnames.sort()  # Sort names in ascending chronological order
    start_dir = os.getcwd()
    # Step 1 - Get all unique station IDs from meta_target_path. Aggregated into n_chunks arrays
    # target_ids = np.unique(pd.read_csv(meta_target_path, sep='\t')['ID'])  # IDs of stations in case study area
    target_ids = np.unique(pd.read_csv(meta_target_path)['ID'])  # IDs of stations in case study area
    n = target_ids.shape[0] / n_chunks  # Number of IDs per set
    chunks = []
    for i in np.arange(n_chunks):
        chunks.append(target_ids[n*i:n*(i+1)])
    chunks[-1] = np.append(chunks[-1], target_ids[n*(i+1):])  # Append the remainder to the last set
    # Step 2 - Iterate through each of the n_chunks of IDs
    for i, chunk in enumerate(chunks):  # Each chunk is a subset of the station IDs to be processed
        print 'Processing chunk number: %d' % i
        # Initiate one large temp dataframe for holding values for all IDs in chunk
        temp_list = []
        # Step 3 - Iterate through all the station data files
        os.chdir(station_path)
        tic = time.time()
        for name in fnames:  # Iterate through each station data file. Each file is typically a unique date.
            print 'Processing ' + name
            temp = pd.read_csv(name, sep=',', header=None, index_col=False)
            # Step 4 - Iterate through all the IDs in the chunk, extract the time series and append to temp_chunk.
            for stat_id in chunk:
                temp_list.append(get_id_time_series(temp, stat_id))  # Time series for stat_id
            del temp  # delete it to clear memory
            # print "Size of temp_list[] = %d" %sys.getsizeof(temp_list)
            gc.collect()
        temp_chunk = pd.concat(temp_list)  # One big time series with all stations in chunk
        del temp_list
        print 'Time to process %f' % (time.time() - tic)
        # Step 5 - Write the output for IDs in the current chunk
        for stat_id in chunk:
            # Write the time series
            os.chdir(out_path)
            os.mkdir(str(stat_id))
            os.chdir(str(stat_id))
            temp_ts = temp_chunk[temp_chunk['Station'] == stat_id]  # time series with just the stat_id
            temp_ts.to_csv('time_series.csv', sep=',', index=False)
            ts_agg_measures(temp_ts).to_csv('summary.csv', sep=',', index=False)
        del temp_chunk  # clear this from memory
    os.chdir(start_dir)


def rollup_time_series(agg_period, station_path, out_name, nrows=105120):
    """
    Used to rollup the raw time series into larger temporal aggregates. By default, the time series will be in 5-minute
    time bins. This method can be used to bin them into 15 or 30 minute bins (or any other aggregation).
    :param agg_period: (int) Defines how many rows to group together during aggregation.
    :param station_path: (str) Path to the directory containing the station time_series.csv
    processed. This directory should have been created by utils.station.get_station_targets().
    :param out_name: (str) Name of output csv to be written in same directory as station_path
    :param nrows: (int) Number of rows that a time series with no missing observations should. Defaults to 105120,
    365*24*60/5
    :return:
    """
    start_dir = os.getcwd()
    os.chdir(station_path)
    ts = pd.read_csv('time_series.csv', sep=',', index_col='Timestamp')
    # Check for missing rows and reindex if needed
    if ts.shape[0] != nrows:
        ts = reindex_timeseries(ts)
    # Generate the rolling harmonic mean
    harm_means = np.empty((ts.shape[0] / agg_period))
    samp_sums = np.empty((ts.shape[0] / agg_period))
    flow_sums = np.empty((ts.shape[0] / agg_period))
    #TODO should we be using ts.resample here?
    for j, i in enumerate(np.arange(0, ts.shape[0], agg_period)):
        end = i + agg_period  # end index of period
        ss = np.sum(ts['Samples'][i:end])  # sum of samples
        samp_sums[j] = ss
        sf = np.sum(ts['Total_Flow'][i:end])  # sum of flows
        flow_sums[j] = sf
        hm = sf / np.sum(np.divide(ts['Total_Flow'][i:end], ts['Avg_Speed'][i:end]))
        harm_means[j] = hm
    # Create output dataframe and write to csv
    out = ts.iloc[np.arange(0, ts.shape[0], agg_period), :]
    out.drop('Avg_Occ', axis=1, inplace=True)
    out.drop('Observed', axis=1, inplace=True)
    out.drop('Samples', axis=1, inplace=True)
    out.drop('Total_Flow', axis=1, inplace=True)
    out.drop('Avg_Speed', axis=1, inplace=True)
    out['Samples_Rollup'] = samp_sums
    out['Total_Flow_Rollup'] = flow_sums
    out['Avg_Speed_Rollup'] = harm_means
    out.to_csv(out_name, header=True, index=True)
    os.chdir(start_dir)


def generate_distributions(ts_df, metric, bins, days=None):
    """
    Reads a station time series (output of station.generate_time_series()) and produces the empirical probabiltiy
    density distribution for the given days of the week.

    :param ts_df: (str) Path to csv file with the station time series. This csv must be the output of
    station.generate_time_series
    :param metric: (str) Identifies the metric for which to generate a distribution. Either 'Count' or 'Speed'
    :param bins: (list) List of bin edges, including lower and upper bins. e.g [0,1,2,3] defines three bins. These bins
    describe the width the metric (e.g. how many mph wide should the speed distribution bins be?)
    :param days: ([int]) Integers identifying the days of the week to create distributions for. Sunday = 0, ...
    Saturday = 6. Defaults to None. If None, all seven days used, days = [0, 1, ... 6]
    :return: ([[df...]]) List of lists of dataframes. Each sublist contains four dataframes: totals (histogram),
    proportions (distribution), variance of totals,
    and variance of proportions.
    """
    if not days:
        days = [0, 1, 2, 3, 4, 5, 6]
    # Read the whole time series and convert the time strings to datetimes
    ts = pd.read_csv(ts_df, sep=',')
    ts['Timestamp'] = pd.to_datetime(ts['Timestamp'])

    # Get the column name for the metric for which disributions are being calculated
    if metric.lower() == 'count':
        metric_col = 'Total_Flow'
    elif metric.lower() == 'speed':
        metric_col = 'Avg_Speed'
    else:
        raise utils.util_exceptions.WrongParamError(
            "The metric parameter must either be 'Count' or 'Speed'"
        )

    # Build a dict to map minutes since midnight to time string, e.g. 65:'01:05'
    hours = [str(x/60) if len(str(x/60)) == 2 else '0'+str(x/60) for x in range(0, 60*24, 5)]
    minutes = [str(x % 60) if len(str(x%60)) == 2 else '0'+str(x%60) for x in range(0, 60*24, 5)]
    time_strs = [x+':'+y for x,y in zip(hours, minutes)]
    time_dict = dict(zip(range(0, 60*24, 5), time_strs))

    # Build the output dataframes
    out = []  # Output list of lists of dataframes
    for day in days:
        # Extract specific day and metric
        ts_temp = ts[ts['Timestamp'].apply(lambda t: t.weekday() == day)][['Timestamp', metric_col]]
        # Add a column that only has the time since midnight 'hh:mm'
        ts_temp['Minutes'] = ts_temp['Timestamp'].apply(lambda t: time_dict[60*t.hour + t.minute])
        ##
        # Totals dataframe
        ##
        # NOTE for some goddamn reason this groupby(*).apply(*) generates a series with shape (288,) where
        # each element is an nd.array. So I then have to coerce it into a dataframe
        series = ts_temp.groupby('Minutes').apply(lambda s: np.histogram(s[metric_col], bins=bins)[0])
        totals = pd.DataFrame([a for a in series], index=series.index, columns=bins[0:-1])
        ##
        # Proportions dataframe
        ##
        proportions = totals.apply(lambda a: a/np.sum(a), axis=1)
        ##
        # Variance of totals dataframe
        ##
        row_totals = totals.sum(axis=1)
        z = zip(row_totals, proportions.values)
        var_tots = pd.DataFrame([np.power(a, 3)/(a-1)*b*(1-b) for a, b in z], index=row_totals.index,
                                columns=proportions.columns)
        ##
        # Variance of proportions
        ##
        var_props = proportions.apply(lambda x: x*(1-x))
        out.append([totals, proportions, var_tots, var_props])
    return out
#TODO improve the interface between generate_distributions and group_days. There is an implicit step, handled in my
# executable, where the output of each call to generate_distributions is written to a subdirectory of specific format
# group_days is expecting that same directory hierarchy.

def group_days(station_dir, days, metric='Both', out_dir=None):
    """
    Takes the output of generate_distributions and aggregates multiple days together.
    :param station_dir: (str) Path to the parent directory holding the day-of-week directories of each distribution.
    WARNING: the csv files in these subdirectories must be the output of generate_distributions()
    :param days: ([int]) List of integers defining days of week to aggregate together. Sunday = 0, Saturday = 6
    :param metric: (str) Defines which metric/s to aggregate. Default is 'Both', meaning both 'Counts' and 'Speed'
    :param out_dir: (str) Path to output directory to write aggregates. If None, an output directory is created in the
    station_dir.
    :return:
    """
    start_dir = os.getcwd()
    os.chdir(station_dir)
    # Get list of directories with day-of-week distributions
    day_dict = {0: '0_Sun', 1: '1_Mon', 2: '2_Tue', 3: '3_Wed', 4: '4_Thur', 5: '5_Fri', 6: '6_Sat'}
    # Create the output directories if do not already exist
    if not os.path.isdir('7_Day_Groups'):
        os.mkdir('7_Day_Groups')
    agg_dir = os.path.abspath('7_Day_Groups')
    os.chdir(agg_dir)
    if not out_dir:
        out_dir = os.path.abspath('_'.join([day_dict[day_int] for day_int in days]))
    try:
        os.mkdir(out_dir)
    except OSError:  # Overwrite existing dirs by same name
        shutil.rmtree(out_dir)
        os.mkdir(out_dir)
    os.chdir(station_dir)
    # Decide which metrics to aggregates
    m_names = {'count': ['counts_totals.csv', 'counts_proportions.csv'],
               'speed': ['speed_totals.csv', 'speed_proportions.csv']}
    if metric.lower() == 'both':
        m_file_names = m_names.values()
    elif metric.lower() == 'count' or metric.lower() == 'speed':
        m_file_names = [m_names[metric.lower()]]
    else:
        raise utils.util_exceptions.WrongParamError(
            'The metric parameter is invalid. Try using: None, Count, or Speed'
        )
    df_lists = [[] for m in m_file_names]  # Each nested list will hold the totals dataframe from each day to be aggregated
    # Open all the dataframes to aggregate and store in nested lists
    for day_int in days:
        os.chdir(day_dict[day_int])  # Move to day-of-week directory to read files
        #  Loop through metrics to aggregate
        for i, m in enumerate(m_file_names):
            df_lists[i].append(pd.read_csv(m[0], sep=',', index_col='Minutes'))
        os.chdir(station_dir)
    #os.chdir(agg_dir)
    # Sum the dataframes and write to output directory
    os.chdir(out_dir)
    for i, dfs in enumerate(df_lists):  # Iterate through each list of dataframes
        totals = dfs[0]
        for d in dfs[1:]:  # Iterate through the sublist and sum
            totals = totals + d
        proportions = totals.apply(lambda a: a/np.sum(a), axis=1)
        totals.to_csv(m_file_names[i][0], sep=',', header=True, index=True)
        proportions.to_csv(m_file_names[i][1], sep=',', header=True, index=True)
    os.chdir(start_dir)

def distribution_trendlines(parent_dir, target_dir, out_dir, metric, write_out=True):
    """
    Reads individual station distribution files and calculates the trendlines, the mean time series, into one output.
    :param parent_dir: (str) Path to the parent directory holding the station-level directories.
    :param target_dir: (str) Relative path, from station_dir to the directory with the distributions. The last
    sub-directory describes the day or day group.
    e.g. if full path is C:\\station_id\\7_Day_Groups\\6_Sat_0_Sun, the target path is 7_Day_Groups\\6_Sat_0_Sun. The
    6_Sat_0_Sun directory contains Saturday-Sunday aggregates.
    :param out_dir: (str) Path to where to write the trendlines file.
    :param metric: (str) Defines which metric to create trendlines for.
    :param write_out: (bool) Default is True. If True, writes the output dataframe to csv. Otherwise returns the dataframe
    without writing.
    :return: (DataFrame) Creates a dataframe of the trendlines  and writes them to a csv in the out_dir directory.
    """
    stations = [n for n in os.listdir(parent_dir) if n.isdigit()]  # List of names of station directories
    # Lookup for files to read based on metric parameter
    m_file_name = get_metric(metric)
    # Seed the output dataframe. All csv files will have the same row labels and column headers.
    #os.chdir(stations[0] + '/' + target_dir)
    tp = os.path.join(parent_dir, stations[0], target_dir)
    days_name = os.path.split(tp)[-1]  # Last directory in the target_dir. Describes the days aggergated
    temp = pd.read_csv(os.path.join(tp, m_file_name), sep=',', header=0, index_col=0)  # Example of input csv
    bins = temp.columns.astype(float)
    delta = bins[1] - bins[0]
    mid_points = bins + delta/2  # List of middle values of each bin
    # Iterate through the stations and build mean time series
    temp_list = []
    for stat in stations:
        print 'Processing %s' % (stat)
        #os.chdir(stat + '/' + target_dir)
        tp = os.path.join(parent_dir, stat, target_dir, m_file_name)
        # Create a the weighted trendline
        temp_list.append(pd.read_csv(tp, sep=',', header=0, index_col=0).
                         apply(lambda x: np.dot(x, mid_points) / np.sum(x), axis=1))
    # Create output directories
    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    #os.chdir(out_dir)
    if not os.path.isdir(os.path.join(out_dir, days_name)):
        os.mkdir(os.path.join(out_dir, days_name))
    # try:
    #     os.mkdir(os.path.join(out_dir, days_name))
    # except OSError:
    #     shutil.rmtree(os.path.join(out_dir, days_name))
    #     os.mkdir(os.path.join(out_dir, days_name))
    #os.chdir(days_name)
    tp = os.path.join(out_dir, days_name, m_file_name.split('.')[0] + '_trendlines.csv')
    # Concat the trendlines into one df and write to output
    out = pd.concat(temp_list, axis=1).transpose()
    out.index = stations
    if write_out:
        out.to_csv(tp)
    return out

def time_period_analysis(parent_dir, target_dir, time_period, metric, out_dir, write_out=True):
    """
    Sums individual station distributions across a time_period. Percentiles and metrics are calculated
    :param parent_dir: (str) Path to the parent directory holding the station-level directories.
    :param target_dir: (str) Relative path, from station_dir to the directory with the distributions. The last
    sub-directory describes the day or day group.
    :param time_period: ((str, str)) 2-Tuple or list containing the start and end time of time period to analyzed. Time
    format is 'hh:mm'
    :param metric: (str) Defines which metric to analyze.
    :param out_dir: (str) Path to where to write the time period analysis.
    :param write_out: (bool) Default is True. If True, writes the output dataframe to csv. Otherwise returns the dataframe
    without writing.
    :return: (None)
    """
    stations = [n for n in os.listdir(parent_dir) if n.isdigit()]  # List of names of station directories
    m_file_name = get_metric(metric)  # Get the metric file name to read
    # Create the output directory
    if not os.path.isdir(out_dir):
        #os.mkdir(out_dir)
        os.makedirs(out_dir)
    time_dir = os.path.join(out_dir, '_'.join(time_period[0].split(':') + time_period[1].split(':')))
    try:
        os.mkdir(time_dir)
    except OSError:  # Overwrite if already exists
        shutil.rmtree(time_dir)
        os.mkdir(time_dir)
    # Find the midpoints of the bins
    tp = os.path.join(parent_dir, stations[0], target_dir, m_file_name)
    temp = pd.read_csv(tp, sep=',', header=0, index_col=0)
    bins = temp.columns.astype(float)
    mid_points = bins + (bins[1] - bins[0])/2.0
    # Loop through each station and extract the time_period
    temp_list = []
    out_stations = []  # List of station ids that will be included in final aggregate
    for stat in stations:
        print 'Processing station %s' % stat
        tp = os.path.join(parent_dir, stat, target_dir, m_file_name)
        # Check if time bin includes midnight, which will cause the "later" time to be a smaller number
        t_start = int(time_period[0].split(':')[0])
        t_end = int(time_period[1].split(':')[0])
        if t_start < t_end:
            # sum over the time period
            temp = pd.read_csv(tp, sep=',', header=0, index_col=0).loc[time_period[0]:time_period[1]].sum()
        else:
            temp = pd.read_csv(tp, sep=',', header=0, index_col=0)
            idx_start = np.searchsorted(temp.index.values, time_period[0])
            idx_end = np.searchsorted(temp.index.values, time_period[1])
            mask = [True] * temp.shape[0]
            mask[idx_end+1:idx_start] = [False]*(idx_start - idx_end - 1)
            temp = temp.loc[mask].sum()
        # Some ramp detectors do not report speed, so we need to check if any speed observations exist:
        if temp.sum():
            # obsvs is vector where each speed is repeated the number of times observed.
            obsvs = np.concatenate([np.repeat(val, n) for val, n in zip(mid_points, temp.values)])
            m = np.mean(obsvs)
            s = np.std(obsvs)
            pct = np.percentile(obsvs, [5, 15, 25, 50, 75, 85, 95])  # percentiles
            temp2 = pd.Series(np.concatenate((pct+[m]+[s], temp.values)))
            temp_list.append(temp2)
            out_stations.append(stat)
    out = pd.concat(temp_list, axis=1).transpose()
    out.index = out_stations
    out.columns = np.concatenate((['5p', '15p', '25p', '50p', '75p', '85p', '95p', 'mean', 'std'],
                                  mid_points.values.astype(str)))
    print mid_points
    if write_out:
        out.to_csv(os.path.join(time_dir, m_file_name.split('.')[0] + '_analysis.csv'), header=True, index=True)



######################################################################################################################
# Helper functions
######################################################################################################################
# Little methods to support the Worker methods

def calc_row_var(x, row_totals):
    coef = np.power(row_totals[x.index], 3)/float(row_totals[x.index])*x*(1-x)

def get_id_time_series(station_df, stat_id):
    """
    Parses a dataframe of extracted station data and extracts a time series of count and speed data for
    one specific ID. The station ID field is named 'Station' in the output dataframe in order to maintain consistency
    with the PeMS documentation nomenclature.
    :param station_df: (pd.DataFrame) Dataframe of extracted station data.
    :param stat_id: (int) ID of specific station to be extracted.
    :return: (pd.DataFrame) Dataframe of the extracted time series.
    """
    # Header of columns to be extracted. Only includes link aggregates, not lane-level data.
    # NOTE: 'Station' is the ID field.
    head = ['Timestamp', 'Station', 'District', 'Fwy', 'Dir', 'Type',
            'Length', 'Samples', 'Observed', 'Total_Flow', 'Avg_Occ', 'Avg_Speed']
    # df is an extract of only rows where station ID = ID
    df = station_df[station_df.iloc[:, 1] == stat_id].iloc[:, 0:len(head)]
    df.columns = head
    return df

def ts_agg_measures(ts_df):
    """
    Calculates the aggregate measures for a time series for a single station ID: Date Range, Number of Observations
    , Standard Deviation of length
    :param ts_df: (pd.DataFrame) Output of get_id_time_series()
    :return: (pd.DataFrame) One-row dataframe with aggregate measures.
    """
    head = ['First_Day', 'Last_Day', 'Total_Observations', 'Length_Std']
    out = pd.DataFrame(columns=head)
    if ts_df.shape[0]:
        out.loc[0, :] = [ts_df['Timestamp'].iloc[0][0:10], ts_df['Timestamp'].iloc[-1][0:10],
                         ts_df.shape[0], np.round(np.std(ts_df['Length']), decimals=2)]
        return out
    else:
        return out

def get_metric(metric):
    # Lookup for files to read based on metric parameter
    m_names = {'count': 'counts_totals.csv',
               'speed': 'speed_totals.csv'}
    if metric.lower() == 'count' or metric.lower() == 'speed':
        return m_names[metric.lower()]
    else:
        raise utils.util_exceptions.WrongParamError(
            'The metric parameter is invalid. Try using: None, Count, or Speed'
        )

def reindex_timeseries(ts_df, start_time_string='05/01/2014 00:00:00', days=365):
    """
    If the time series is missing observations, inserts a row with NaN values. That way every dataframe is the same
    size.
    :param ts_df: (pd.DataFrame)
    :return: (pd.DataFrame)
    """
    # Make the index for a full day
    start_datetime = datetime.datetime.strptime(start_time_string, "%m/%d/%Y %H:%M:%S")
    delta = datetime.timedelta(minutes=5)
    time_index = [(start_datetime + i*delta).strftime("%m/%d/%Y %H:%M:%S") for i in np.arange(days*24*60/5)]
    # Return a new dataframe that has been reindexed to the full set of observations. Missing rows will have NaN vals
    return ts_df.reindex(time_index, method=None, copy=True)
