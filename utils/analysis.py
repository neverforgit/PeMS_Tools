__author__ = 'acampbell'

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

"""
These tools are used to analyze processed PeMS station data. In general, they should be used on the things produced
by the utils.extractor tools.
"""

######################################################################################################################
# Worker functions
######################################################################################################################
# These are the methods that do all the heavy lifting

def plot_daily_station_series(df, station, out_path, field, fs=(10,6)):
    """
    Creates and saves daily time series plots for one field for one station.
    :param df: (pd.DataFrame) Data frame of combined station data files. Typically will be output
    from utils.extractor.station_files_to_df
    :param station: (int) Numeric id of station to plot data for.
    :param out_path: (str) Path to directory to write the images.
    :param field: (str) Column name of field from df to plot.
    :param fs: (tuple) Size of output figures.
    :return: (None)

    """
    station_df = df[df['Station'] == station][['Timestamp', field]]
    # If the dataframe has not already converted timestamps to datetimes, convert them now
    if station_df.dtypes['Timestamp'] != np.dtype('<M8[ns]'):
        station_df['Timestamp'] = pd.to_datetime(station['Timestamp'])
    dates = np.unique(station_df['Timestamp'].apply(lambda x: x.date()))  # Unique dates
    for d in dates:
        idx = station_df['Timestamp'].apply(lambda x: x.date() == d)
        plt.close('all')
        fig = plt.figure(figsize=fs)
        ax = fig.gca()
        try:
            plt.plot(station_df[idx]['Timestamp'], station_df[idx][field], ':bo')
            plt.ylabel(field)
            fig.autofmt_xdate()
            ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
        except ValueError:
            continue
        plt.title('Station: %d, %s' %(station, d.isoformat()))
        plt.savefig('%s%s_%s_%s.png' % (out_path, str(station), field, d.isoformat()))
        plt.close()




######################################################################################################################
# Helper functions
######################################################################################################################
# Little functions to support the Worker methods

