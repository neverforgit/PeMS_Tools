import time
import csv

import webbrowser

with open('Input_data/d7_2013_downloadlinks.csv', mode='r') as infile:
    reader = csv.reader(infile);
    pagesd4 = [];
    for entry in reader:
    		pagesd4.append(entry[0]);   

for page in pagesd4:
	print page
	webbrowser.open_new("http://pems.dot.ca.gov/"+page)
	time.sleep(90)