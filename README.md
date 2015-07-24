# PeMS_scraper
Tools and executables for downloading and processing data from PeMS.

Tasks:
1) Add reference to GHub repo to file headers
2) Rename repo since this thing has grown to much more then web scraping
3) Organize the utils.* modules better. Do I want to have specific modules for each PeMS data source, 
or should they be grouped by type of behavior (download, extract, analyze...)

Bugs:
1) The date column produced by utils.health.process_joined is offset back by one day. e.g. real date is 2/20
  date in column is 2/19
2) 
