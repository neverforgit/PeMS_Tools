__author__ = 'Andrew Campbell'

import os, shapefile

import pandas as pd

from shapely.geometry import Polygon, Point
from utils import util_exceptions

"""
These tools are used for processing Station 5-Minute raw data files. These file are reported at the district  level of
spatial aggregation, which may be too big or small for the level of analysis. These tools allow you to extract the rows
that fall within a shapefile polygon that defines your casestudy area.
"""

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

def get_meta_targets(meta_path, shape_path, out_path="", write_out=False,  preamble='d04_text_meta'):
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

    for name in fnames:
        temp = pd.read_csv(name, sep='\t')
        # Extract only the values within the poly
        temp = temp[temp.apply(lambda x: in_poly(x, poly), axis=1)]
    #TODO figure out what to do with the extracted dataframe. Should I make one big dataframe consisting all the subsets extracted from raw files. Or create new subsampled files?

    os.chdir(start_dir)

def in_poly(row, poly):
    """
    Helper function to be applied to station metadata dataframe row.
    :param row: (pd.Series) A single row of the station metadate dataframe
    :param poly: (Polygon) Shapely Polygon
    :return: (boolean) True is the x,y coords in the row are withing the poly.
    """
    return poly.contains(Point(row['Longitude'], row['Latitude']))

