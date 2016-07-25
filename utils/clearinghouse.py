from datetime import date, timedelta
from HTMLParser import HTMLParser
import logging
import time

from numpy.random import random_integers
from requests import session, ConnectionError

__Author__ = "Andrew A Campbell"

"""
These tools are used to automate the downloading of files from the Data Clearinghouse. Within the PeMS website,
you use the gui to generate a set of download links. Once you have the right links displayed, download the
html and pass it this script in the config file. The script will then parse out all the download links
and download them automoatically.
"""


class MyHTMLParser(HTMLParser):
    """
    Parser to extract download links
    """

    def __init__(self, dl_links=None):
        HTMLParser.__init__(self)
        if dl_links is None:
            self.dl_links = []
        else:
            self.dl_links = dl_links

    def handle_starttag(self, tag, attrs, key_string="download"):
        if tag == 'a' and 'href' in [at[0] for at in attrs]:
            link = dict(attrs).get('href')
            #  Check if it is a download link
            if key_string in link:
                self.dl_links.append(dict(attrs).get('href'))


def daterange(start_date, end_date):
    """
    Tool for iterating over range of dates.
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def download_links(link_list, dt, out_path, url_base='http://pems.dot.ca.gov/'):
    """
    Downloads the list of links from the PeMS clearinghouse.

    Args:
        link_list (list): List of links to files to download
        dt (dict): Data dict to be used in POST request. Includes
            username and password, which are spec
    Returns:
        int: Number of files downloaded
    """
    with session() as c:
        #  POST the login request
        c.post(url_base, data=dt)
        for i, link in enumerate(link_list):
            ts = 10  # Default time to sleep
            print "Iteration: " + str(i)
            logging.info('initial time to sleep ' + str(ts))
            while True:
                try:  # Download with 10-second sleep time breaks
                    print 'Downloading file number: ' + str(i)
                    logging.info('try to download file: ' + str(i))
                    logging.info('time to sleep ' + str(ts))
                    # Make the request and download attached file
                    r = c.get(link)
                    fname = r.headers['content-disposition'].split('=')[-1]
                    with open(out_path + fname, 'wb') as fi:
                        fi.write(r.content)
                    time.sleep(random_integers(ts, int(1.2 * ts)))
                except ConnectionError:
                    logging.warning('ConnectionError')
                    ts = ts * 2
                    time.sleep(ts)  # Sleep and login again
                    c.post(url_base, data=dt)  # , params=p)
                    continue
                break


