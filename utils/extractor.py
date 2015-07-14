__author__ = 'Andrew Campbell'

import os

import pandas as pd

"""
These tools are used for processing Station 5-Minute raw data files. These file are reported at the district  level of
spatial aggregation, which may be too big or small for the level of analysis. These tools allow you to extract the rows
that fall within a shapefile polygon that defines your casestudy area.
"""

def get_unique_xy(meta_path, county=75, preamble='d04_text_meta'):
    '''
    :param meta_path: (str) Path to directory containing station metadata files
    :param preamble: (str) The the leading set of characters that all metadata files will have.
                     This allows us to avoid reading hidden files and other cruff.
    :return: (pd.DataFrame) A dataframe of all unique lat, lon coordinates
    '''
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
