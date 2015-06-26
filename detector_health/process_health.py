import re
import pickle

from utils.health import *

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "ERROR: you must provide the path to the data directory as a system arg."
        exit
    else:
        data_path = sys.argv[1]
    #data_path = "C:\PeMS_scraper\detector_health\data_health"

    # List of all data files in data_path dir
    fnames = [f for f in os.listdir(data_path) if re.match('[0-9]+_[0-9]+_[0-9]+_', f)]
    fnames.sort()
    year = int(fnames[0].split('_')[0])
    ##
    #  Step 1 - join all data files into one data frame and csv
    ##
    print 'Joining all raw files into one DataFrame and csv.'
    df = join_all(data_path, '.')  # will take several minutes
    with open(str(year)+'_joined_health_detail.p', 'w') as fo:
        pickle.dump(df, fo)
    print 'Joined DataFrame saved.'

    ##
    #  Step 2 - Build table of daily health for all detectors
    ##


    # days = daterange(date(year,1,1), date(year,12,31)) #  List of all days in year
    days = [d.split('_health')[0] for d in fnames]
    df = pd.DataFrame(columns=['VDS', 'Year_Avg'] + days)  # Initiate empty data frame

    [process_day(d, df) for d in fnames]  # Process all the days
