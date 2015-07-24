__author__ = 'Andrew Campbell'

import os

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
    Extracts readings from sensors that fall within the polygon defining the case study area.
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





######################################################################################################################
# Helper functions
######################################################################################################################
# Little methods to support the Worker methods




