__author__ = 'acampbell'

import os
from datetime import datetime

import numpy as np
import pandas as pd
import shapefile
from shapely.geometry import Polygon, Point

import util_exceptions

"""
These tools are used for processing station metadata files.
"""

######################################################################################################################
# Worker functions
######################################################################################################################
# These are the functions that do all the heavy lifting

def join_meta(meta_path, start_date=None, end_date=None,
              out_path=None, write_out=False, preamble='d04_text_meta_', concat_intv=10):
    """
    Combines all the metadata files into one big dataframe and writes is to a csv with an appended date file.
    There is an option to use start and end dates so as to filter out files that do not fall within that daterange.
    :param meta_path: (str) Path to directory with metadata files to join.
    :param start_date: (datetime) Optional, earliest date of files to join.
    :param end_date: (datetime) Optional, latest date of files to join.
    :param out_path: (str) Full path of output file.
    :param write_out: (bool) Set to False to suppress writing of output csv. Default it False.
    :param preamble: (str) Leading text of metadata files. Used to avoid reading hidden OS files.
    :return: (pd.DataFrame) Data frame of vertically stacked metadata files. A date column is appendend.
    """
    start_dir = os.getcwd()
    os.chdir(meta_path)
    fnames = [n for n in os.listdir('.') if n[0:len(preamble)] == preamble]  # List of all file name to read
    fnames.sort()  # Sorting names ensures the dates will be read chronologically
    temp_list = []
    # If start_date and end_date provided, truncate fnames to only include files falling within those dates
    if start_date and end_date:
        temp = []
        for name in fnames:
            parts = name.split('_')
            d = datetime(int(parts[3]), int(parts[4]), int(parts[5].split('.')[0]))
            if start_date <= d <= end_date:
                temp.append(name)
        fnames = temp
    for name in fnames:
        temp = pd.read_csv(name, sep='\t')
        d = name.split('.')[0][-10:]
        temp['Date'] = d  # Date of the metadata file
        temp['ID'].astype(int)  # Cast station ID to int
        temp_list.append(temp)
        if len(temp_list) == concat_intv:
            temp_list = [pd.concat(temp_list)]
    os.chdir(start_dir)
    df = pd.concat(temp_list, ignore_index=True)
    if write_out:
        df.to_csv(out_path, sep='\t', index=False)
    return df

def get_meta_targets_from_files(meta_path, shape_path, out_path='', write_out=False,  preamble='d04_text_meta'):
    """
    Extracts all rows from station metadata files where the station falls within the polygon defined by the shapefile.
    :param meta_path: (str) Path to directory of station metadata files.
    :param shape_path: (str) Path to the polygon shapefile. Shapefile must contain only one polygon.
    :return: (None)
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
        temp_list.append(temp_df(name, poly))
    df = pd.concat(temp_list, ignore_index=True)
    if write_out:
        df.to_csv(out_path, sep='\t', index=False)
    os.chdir(start_dir)
    return

def get_meta_targets_from_df(meta_df, shape_path):
    """
    Extracts all rows from dataframe of joined metadata files where the station falls within the
    polygon defined by the shapefile.
    :param meta_df: (pandas.DataFrame) Dataframe of joined metadata files. Must be output of meta.join_meta()
    :param shape_path: (str) Path to the polygon shapefile. Shapefile must contain only one polygon.
    :return: (pandas.DataFrame) Dataframe that is a subset of the input dataframe. Only contains rows of stations
            within the polygon boundaries.
    """
    # Create Shapely polygon
    poly_points = shapefile.Reader(shape_path).shapes()[0].points
    poly = Polygon(poly_points)
    return meta_df[meta_df.apply(lambda x: in_poly(x, poly), axis=1)]

def get_uniqe_id_locs(meta_df):
    """
    Takes a dataframe of joined metadata files (output of join_meta) and returns a dataframe of all the unique 3-tuples of
    (ID, Latitude, Longitude)
    :param meta_df: (pd.DataFrame) Pandas Dataframe of joined metadata files. Must be the output of
    utils.meta.join_meta() to ensure the date column with proper format is present.
    :return: (pd.DataFrame) Dataframe of all the unique 3-tuples (ID, Latitude, Longitude). Date is the first appearance
            of the unique 3-tuple
    """
    return meta_df[['ID', 'Latitude', 'Longitude', 'Date']].sort('Date').\
        drop_duplicates(['ID', 'Latitude', 'Longitude'])

def filter_moving_ids(meta_df):
    """
    Due to bugs in the PeMS database, the mapping of station ID to Latitude and Longitude is not consistent. Some
    stations "move". Since data from these stations cannot be trusted, this script will remove them.
    :param meta_df: (pd.DataFrame) Pandas Dataframe of joined metadata files. Must be the output of
    utils.meta.join_meta() to ensure the date column with proper format is present.
    :return: (pd.DataFrame) Subset of input with moving stations removed.
    """
    unique_id_locs = get_uniqe_id_locs(meta_df)
    unique_id = unique_id_locs.drop_duplicates(['ID'])
    idx = [i for i in unique_id_locs.index if i not in unique_id.index]  # Indices of IDs that appear more than
    moving_ids = np.unique(unique_id_locs.loc[idx, 'ID'])  # Series of station ID values of moving stations
    return meta_df[meta_df.apply(lambda x: x['ID'] not in moving_ids, axis=1)]

def get_unique_xy(meta_path, county=75, preamble='d04_text_meta'):
    """
    :param meta_path: (str) Path to directory containing station metadata files
    :param preamble: (str) The the leading set of characters that all metadata files will have.
                     This allows us to avoid reading hidden files and other cruff.
    :return: (pd.DataFrame) A dataframe of all unique 5-tuples (lat, lon, dir, fwy, type)
    """
    #TODO rewrite this using df.drop_duplicates(). I can be a one or two line function! See utils.meta.get_unique_ID_locs
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