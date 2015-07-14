import re
import pickle
import time
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from utils.health import *
from datetime import date

if __name__ == "__main__":
    #TODO instead of sys args, use a config file for all the different directory paths
    if len(sys.argv) < 2:
        print "ERROR: you must provide two arguments: path to data directory and path to output directory."
        exit
    else:
        data_path = sys.argv[1]
        out_path = sys.argv[2]
    #data_path = "C:\PeMS_scraper\detector_health\data_health"

    # List of all data files in data_path dir
    fnames = [f for f in os.listdir(data_path) if re.match('[0-9]+_[0-9]+_[0-9]+_', f)]
    fnames.sort()
    year = int(fnames[0].split('_')[0])
    ##
    #  Step 1 - join all data files into one data frame and csv
    ##
    t0 = time.time()
    print 'Joining all raw files into one DataFrame and csv.'
    df = join_all(data_path, '.')  # will take several minutes
    with open(out_path+str(year)+'_joined_health_detail.p', 'w') as fo:
        pickle.dump(df, fo)
    print 'Joined DataFrame saved.'
    print 'Time to join files: ' + str(time.time()-t0) + ' [sec]'

    ##
    #  Step 2 - Build table of daily health for all detectors
    ##
    df.loc[:, 'Status'] = df.loc[:, 'Status'].astype(int)
    #  Create a n x 365 dataframe where there are n-unique VDS stations.
    #  Each cell is the average health for that day
    g = df.loc[:,'Status'].groupby([df.loc[:,'VDS'], df.loc[:,'Date']]).agg(lambda x: float(np.sum([xx==0 for i,xx in enumerate(x)]))/float(len(x))).unstack()
    g.loc[:, 'Year_Avg_Status'] = g.apply(lambda x: np.mean(x), axis=1)  # Avg. health for the whole year
    g.loc['Daily_Avg',:] = g.apply(np.mean)
    with open('2014_daily_health.pl', 'w') as f: pickle.dump(g, f)
    g.to_csv('2014_daily_health.txt', sep='\t')

    ##
    # Step 3 - make time series plots of all sensor's daily health
    ##
    start_date = date(2014, 01, 01)
    end_date = date(2014, 12, 31)
    dates = daterange(start_date, end_date, 1)
    out_path_health_imgs = 'C:\PeMS_scraper\data_health_plots'
    plt.ioff()  # Turn off interactive mode to hopefully supress displaying of plots
    for x in g.iterrows():
        plot_VDS_series(x[1][0:365], out_path_health_imgs, dates)

    ##
    # Move the SF plots to a separate folder
    ##



