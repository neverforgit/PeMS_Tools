def download_routes(config_path, delta=45):
    url_base = 'http://pems.dot.ca.gov/' #  base url for all PeMS queries    
    ##
    #  Load config 
    ##
    conf = ConfigParser()
    conf.read(config_path)
    #  paths
    out_path = conf.get('Paths', 'out_path')
    log_path = conf.get('Paths', 'log_path')
    #  date range
    ds = [int(x) for x in conf.get('Routes', 'start_date').replace(' ', '').split(',')]
    de = [int(x) for x in conf.get('Routes', 'end_date').replace(' ', '').split(',')]
    start_date = date(ds[0], ds[1], ds[2])
    end_date = date(de[0], de[1], de[2])
    #  route ids
    route_ids = conf.get('Routes', 'route_ids').replace(' ', '').split(',')
    #  request data packet
    dt = {
        'action': 'login',
        'username': conf.get('Creds', 'username'),
        'password': conf.get('Creds', 'password')
        }
    #  url query string p
    p = {t[0]:t[1] for t in conf.items('Payload')}
    
    ##
    # Open session and download them filez
    ##    
    with session() as c:
        #  POST the login request
        c.post(url_base, data=dt) #, params=p)
        dates = daterange(start_date, end_date)
        for rid in route_ids:
            print "Route ID:" + str(rid)
            p['route_id'] = rid
            #  Iterate through delta-day long blocks
            for i, (ds,de) in enumerate(zip(dates[0:-1], dates[1:])):
                ts = 10 #  Default time to sleep
                logging.info('initial time to sleep ' + str(ts))
                while True:
                    try:
                        print "Iteration: " + str(i)
                        logging.info('route_id: ' + rid)
                        logging.info('start_date: ' + str(ds))
                        logging.info('iteration '  + str(i))
                        logging.info('time to sleep ' + str(ts))
                        # Update the dynamic payload parameters
                        tds = str(int((ds - date(1970,1,1)).total_seconds()))
                        tde = str(int((de - date(1970,1,1)).total_seconds()))
                        p['s_time_id']=tds; p['s_mm']=str(ds.month); p['s_dd']=str(ds.day); p['s_yy']=str(ds.year)
                        p['e_time_id']=tde; p['e_mm']=str(de.month); p['e_dd']=str(de.day); p['e_yy']=str(de.year)
                        r = c.request('GET', url_base, params=p)
                        #TODO change the output naming convention
                        with open(out_path + str(rid) + '_' + str(ds.year) + '_' + str(ds.month) + '_' + str(ds.day) + '_' + 'route.txt', 'wb') as fi: fi.write(r.text)
                        time.sleep(random_integers(ts,int(1.2*ts)))
                    except ConnectionError:
                        logging.warning('ConnectionError')
                        ts = ts*2
                        time.sleep(ts) #  Sleep and login again
                        c.post(url_base, data=dt) #, params=p)
                        continue
                    break
            #  Iterate  through the remainder days