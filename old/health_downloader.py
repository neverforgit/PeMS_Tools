import time
import logging
from datetime import date, timedelta
from requests import session, ConnectionError
from numpy.random import random_integers

#TODO all of these constants in the beginning should be read from a config file

## 
# Define paths and logging
##
out_path = 'C:\\Users\\acampbell\\Documents\\FCMS\\PeMS\\data\\health_detail'
logging.basicConfig(filename=out_path+'log.log', level=logging.DEBUG)

##
# Configure the request urls and parameters
##
url_base = 'http://pems.dot.ca.gov/'
start_date = date(2014, 1, 21)
end_date = date(2015, 1, 1)


dt = {
        'action' : 'login',
        'username' : 
        'password':
}

p = {
     'report_form': '1',
     'dnode': 'District',
     'content': 'detector_health',
     'tab': 'dh_detail',
     'export': 'text',
     'district_id': '4',
     's_time_id': None,
     's_mm': None,
     's_dd': None,
     's_yy': None,
     'st_cd': 'on',
     'st_ch': 'on',
     'st_ff': 'on',
     'st_hv': 'on',
     'st_ml': 'on',
     'st_fr': 'on',
     'st_or': 'on',
     'filter': 'all',
     'eqpo': '',
     'tag': ''
}



def daterange(start_date, end_date):
    """
    Tool for iterating over range of dates.
    """
    for n in range(int ((end_date - start_date).days)):

        yield start_date + timedelta(n)
        
with session() as c:
    #  POST the login request
    c.post(url_base, data=dt) #, params=p)
    for i,d in enumerate(daterange(start_date, end_date)):
        ts = 10 #  Default time to sleep
        logging.info('initial time to sleep ' + str(ts))
        while True:
            try:
                # print 'date iteration '  + str(i)
                # print 'time to sleep ' + str(ts)
                logging.info('date iteration '  + str(i))
                logging.info('time to sleep ' + str(ts))
                # Update the dynamic payload parameters
                td = str(int((d - date(1970,1,1)).total_seconds()))
                p['s_time_id']=td; p['s_mm']=str(d.month); p['s_dd']=str(d.day); p['s_yy']=str(d.year)
                r = c.request('GET', url_base, params=p)
                month = str(d.month) if d.month > 9 else '0'+str(d.month)
                day = str(d.day) if d.day > 9 else '0'+str(d.day)
                with open(out_path + str(d.year) + '_' + month + '_' + day + '_' + 'health_detail.txt', 'wb') as fi: fi.write(r.text)
                time.sleep(random_integers(ts,int(1.2*ts)))
            except ConnectionError:
                logging.warning('ConnectionError')
                ts = ts*2
                time.sleep(ts) #  Sleep and login again
                c.post(url_base, data=dt) #, params=p)
                continue
            break