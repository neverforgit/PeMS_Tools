__author__ = 'Andrew Campbell'

import os, shapefile

import pandas as pd
import numpy as np

from shapely.geometry import Polygon, Point
from utils import util_exceptions

"""
These tools are used for processing Station 5-Minute raw data files. These files are reported at the district  level of
spatial aggregation, which may be too big or small for the level of analysis. These tools allow you to extract the rows
that fall within a shapefile polygon that defines your casestudy area.
"""

######################################################################################################################
# Worker functions
######################################################################################################################
# These are the functions that do all the heavy lifting

def get_unique_xy(meta_path, county=75, preamble='d04_text_meta'):
    """
    :param meta_path: (str) Path to directory containing station metadata files
    :param preamble: (str) The the leading set of characters that all metadata files will have.
                     This allows us to avoid reading hidden files and other cruff.
    :return: (pd.DataFrame) A dataframe of all unique 5-tuples (lat, lon, dir, fwy, type)
    """
    start_dir = os.getcwd()
    os.chdir(meta_path)
    fnames = [n for n in os.listdir(meta_path) if n[0:13] == preamble]
    points = set()
    for name in fnames:
        temp = pd.read_csv(name, sep='\t')
        temp = temp[temp['County'] == county]
        points = points.union(set(zip(temp['Latitude'],
                                      temp['Longitude'],
                                      temp['Dir'],
                                      temp['Fwy'],
                                      temp['Type'])))
    lat, lon, dir, fwy, tpe = [], [], [], [], []
    for p in points:
        lat.append(p[0])
        lon.append(p[1])
        dir.append(p[2])
        fwy.append(p[3])
        tpe.append(p[4])
    os.chdir(start_dir)
    return pd.DataFrame({'Latitude': lat,
                         'Longitude': lon,
                         'Dir': dir,
                         'Fwy': fwy,
                         'Type': tpe})

def get_meta_targets(meta_path, shape_path, out_path='', write_out=False,  preamble='d04_text_meta'):
    """
    Extracts all rows from station metadata files where the station falls within the polygon defined by the shapefile.
    :param meta_path: (str) Path to directory of station metadata files.
    :param shape_path: (str) Path to the polygon shapefile. Shapefile must contain only one polygon.
    :return:
    """
    if write_out == True and out_path == "":
        raise util_exceptions.MissingParamError(
            "Missing out_path"
        )
    start_dir = os.getcwd()
    os.chdir(meta_path)
    fnames = [n for n in os.listdir(meta_path) if n[0:13] == preamble]

    # Create Shapely polygon
    poly_points = shapefile.Reader(shape_path).shapes()[0].points
    poly = Polygon(poly_points)

    temp_list = []
    for name in fnames:  # Add the rest
        temp_list.append(temp_df(name,poly))
    df = pd.concat(temp_list, ignore_index=True)
    if write_out:
        df.to_csv(out_path, sep='\t')
    os.chdir(start_dir)
    return

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

def in_poly(row, poly):
    """
    Helper function to be applied to station metadata dataframe row.
    :param row: (pd.Series) A single row of the station metadate dataframe
    :param poly: (Polygon) Shapely Polygon
    :return: (boolean) True is the x,y coords in the row are withing the poly.
    """
    return poly.contains(Point(row['Longitude'], row['Latitude']))

def temp_df(name, poly):
    """
    Helper function to read a metadata file into a dataframe and append a date column
    :param name (str) Name of specific metadata file to open
    :param poly: (Polygon) Shapely Polygon
    :return: (dataframe)
    """
    temp = pd.read_csv(name, sep='\t')
    # Extract only the values within the poly
    temp = temp[temp.apply(lambda x: in_poly(x, poly), axis=1)]
    temp['Date'] = name[-14:-4]  # File names are fixed width
    return temp


