# PeMS_scraper
Tools and executables for downloading and processing data from PeMS.

Tasks:
0) Generate the docs:  https://www.jetbrains.com/pycharm/help/documenting-source-code-in-pycharm.html  
1) Add reference to GHub repo to file headers  
2) Rename repo since this thing has grown to much more then web scraping  
3) Organize the utils.* modules better. Do I want to have specific modules for each PeMS data source, 
or should they be grouped by type of behavior (download, extract, analyze...)  
4) Update station extractor methods to: 1) take list of directories instead of just one, 2) filter by date  
   The current version of utils.extractor.get_station_targets simply processes every file in a certain folder  
5) Create a separate utils.aggregate module for the aggregation methods  

Bugs:  
1) The date column produced by utils.health.process_joined is offset back by one day. e.g. real date is 2/20
  date in column is 2/19  
2)   
