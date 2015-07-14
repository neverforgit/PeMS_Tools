import time
import logging
import sys
from datetime import date, timedelta
from requests import session, ConnectionError
from numpy.random import random_integers
from ConfigParser import ConfigParser

"""
Downloads performance time series at 5 minute intervals for a list of route ids and range of dates.
"""

def daterange(start_date, end_date, delta=1):
    """
    Tool for iterating over range of dates.
    Returns a list of the starting date of each new query.
    """
    out = []
    days = (end_date - start_date).days
    for n in range(days/delta+1):
        out.append(start_date + timedelta(n*delta))
    return out
    # for d in range(1, days % delta+1): #  get the remainder days
        # yield start_date + timedelta(n*delta + d)
        
class HealthDownloader():

    def __init__(self):
        self.url_base = 'http://pems.dot.ca.gov/' #  base url for all PeMS queries    
        self.out_path = None
        self.dt = None  # request data
        self.p = None  # request payload
        self.c = None  # session
        
    def open_session(self):
        self.c = session()
        self.c.post(self.url_base, data=self.dt)
        
    def download_health(self, ds):
        """
        ds (date) = start date
        """
        #  Must first open session with open_session() to log in to PeMS
        ts = 10  # Default time to sleep
        logging.info('initial time to sleep ' + str(ts))
        while True:
            try:
                logging.info('start_date: ' + str(ds))
                logging.info('time to sleep ' + str(ts))
                #  Update the dynamic payload parameters
                tds = str(int((ds - date(1970, 1, 1)).total_seconds()))
                self.p['s_time_id']=tds; self.p['s_mm']=str(ds.month); self.p['s_dd']=str(ds.day); self.p['s_yy']=str(ds.year)
                #  Make the new request and save the returned content
                r = self.c.request('GET', self.url_base, params=self.p)
                month = str(ds.month) if ds.month > 9 else '0'+str(ds.month)
                day = str(ds.day) if ds.day > 9 else '0'+str(ds.day)
                with open(self.out_path + str(ds.year) + '_' + month + '_' + day + '_' + 'health_detail.txt', 'wb') as fi: fi.write(r.text)
                time.sleep(random_integers(ts, int(1.2*ts)))
            except ConnectionError:
                logging.warning('ConnectionError')
                ts = ts*2
                time.sleep(ts) #  Sleep and login again
                self.c.post(self.url_base, data=self.dt) #, params=p)
                continue
            break

if __name__ == "__main__":
    if len(sys.argv)<2:
        print "ERROR: need to pass path to config file as sys arg"
        exit
    config_path = sys.argv[1]

    ##
    #  Load config and start logger
    ##
    conf = ConfigParser()
    conf.read(config_path)
    #  date range
    ds = [int(x) for x in conf.get('Dates', 'start_date').replace(' ', '').split(',')]
    de = [int(x) for x in conf.get('Dates', 'end_date').replace(' ', '').split(',')]
    start_date = date(ds[0], ds[1], ds[2])
    end_date = date(de[0], de[1], de[2])
    #  logging
    logging.basicConfig(filename=conf.get('Paths', 'log_file_path'), level=logging.DEBUG)
    
    ##
    # Initialize the downloader and populate fields
    ##
    hd = HealthDownloader()
    hd.out_path = conf.get('Paths', 'out_dir_path')
    hd.dt = {t[0]:t[1] for t in conf.items('Creds')}
    hd.p = {t[0]:t[1] for t in conf.items('Payload')}
    
    ##
    #  Loop through dates and download files
    ##
    hd.open_session() #  Log in to PeMS
    dates = daterange(start_date, end_date)
    for ds in dates:
        print "start date: " + str(ds)
        hd.download_health(ds)
