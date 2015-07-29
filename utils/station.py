__author__ = 'Andrew Campbell'

import os
import time

import numpy as np
import pandas as pd

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
    filtering should be done in the creation of this aggregate metadata file.
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


######################################################################################################################
# Helper functions
######################################################################################################################
# Little methods to support the Worker methods

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
