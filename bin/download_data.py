from ConfigParser import ConfigParser
import logging
import sys

import utils.clearinghouse

"""
This script is used to automate the downloading of 5-minute station files from the Data Clearinghouse. Within the PeMS website,
you use the gui to generate a set of download links. Once you have the right links displayed, download the
html and pass it this script in the config file. The script will then parse out all the download links
and download them automatically
"""

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print "ERROR: need to provide path to config file."
        exit
    config_path = sys.argv[1]

    # Load config constants
    config = ConfigParser()
    config.read(config_path)
    html_path = config.get('Paths', 'html_file_path')
    log_path = config.get('Paths', 'log_file_path')
    out_path = config.get('Paths', 'out_dir_path')
    username = config.get('Creds', 'username')
    pwd = config.get('Creds', 'password')

    # Start logger
    logging.basicConfig(filename=log_path, level=logging.DEBUG)

    # Parse HTML for download links
    logging.info('Parsing HTML to extract download links')
    with open(html_path, 'r') as fi:
        html_string = fi.read()
    parser = utils.clearinghouse.MyHTMLParser()
    parser.feed(html_string)

    # Download all the links
    dt = {
        'action': 'login',
        'username': username,
        'password': pwd
    }
    utils.clearinghouse.download_links(parser.dl_links, dt, out_path)
