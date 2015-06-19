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

def daterange(start_date, end_date, delta=45):
    """
    Tool for iterating over range of dates.
    Yields the starting date of each new query.
    """
    out = []
    days = (end_date - start_date).days
    for n in range(days/delta+1):
        out.append(start_date + timedelta(n*delta))
    return out
    # for d in range(1, days % delta+1): #  get the remainder days
        # yield start_date + timedelta(n*delta + d)
        
class RouteDownloader():

    def __init__(self):
        self.url_base = 'http://pems.dot.ca.gov/' #  base url for all PeMS queries    
        self.out_path = None
        self.dt = None #  request data
        self.p = None #  request payload
        self.c = None #  session
        
    def open_session(self):
        self.c = session()
        self.c.post(self.url_base, data=self.dt)
        
    def download_routes(self, ds, de):
        """
        rid (str) = route id
        ds (date) = start date
        de (date) = end date
        """
        if ds == de:
            return None
        #  Must first open session with open_session() to log in to PeMS
        ts = 10 #  Default time to sleep
        logging.info('initial time to sleep ' + str(ts))
        while True:
            try:
                logging.info('route_id: ' + rid)
                logging.info('start_date: ' + str(ds))
                logging.info('time to sleep ' + str(ts))
                #  Update the dynamic payload parameters
                tds = str(int((ds - date(1970,1,1)).total_seconds()))
                tde = str(int((de - date(1970,1,1)).total_seconds()) + 3600*23 + 60*59 + 59) 
                self.p['s_time_id']=tds; self.p['s_mm']=str(ds.month); self.p['s_dd']=str(ds.day); self.p['s_yy']=str(ds.year)
                self.p['e_time_id']=tde; self.p['e_mm']=str(de.month); self.p['e_dd']=str(de.day); self.p['e_yy']=str(de.year)
                #  Make the new request and save the returned content
                r = self.c.request('GET', self.url_base, params=self.p)
                with open(self.out_path + str(rid) + '_' + str(ds.year) + '_' + str(ds.month) + '_' + str(ds.day) + '_' + 'route.txt', 'wb') as fi: fi.write(r.text)
                time.sleep(random_integers(ts, int(1.2*ts)))
            except ConnectionError:
                logging.warning('ConnectionError')
                ts = ts*2
                time.sleep(ts) #  Sleep and login again
                c.post(self.url_base, data=dt) #, params=p)
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
    ds = [int(x) for x in conf.get('Routes', 'start_date').replace(' ', '').split(',')]
    de = [int(x) for x in conf.get('Routes', 'end_date').replace(' ', '').split(',')]
    start_date = date(ds[0], ds[1], ds[2])
    end_date = date(de[0], de[1], de[2])
    #  route ids
    route_ids = conf.get('Routes', 'route_ids').replace(' ', '').split(',')
    #  logging
    logging.basicConfig(filename=conf.get('Paths', 'log_path'), level=logging.DEBUG)
    
    ##
    # Initialize the downloader and populate fields
    ##
    rd = RouteDownloader()
    rd.out_path = conf.get('Paths', 'out_path')
    rd.dt = {
        'action': 'login',
        'username': conf.get('Creds', 'username').strip(),
        'password': conf.get('Creds', 'password').strip()
        }
    rd.p = {t[0]:t[1] for t in conf.items('Payload')}
    
    ##
    #  Loop through route ids and dates and download files
    ##
    rd.open_session() #  Log in to PeMS
    dates = daterange(start_date, end_date)
    for rid in route_ids:
        rd.p['route_id'] = rid
        print "route id: " + rid
        #  Iterate through delta day long blocks
        for ds,de in zip(dates[0:-1], dates[1:]):
            print "start date: " + str(ds)
            rd.download_routes(ds, de - timedelta(1))
        #  Download remaining block of days
        rd.download_routes(de, end_date)
   
    