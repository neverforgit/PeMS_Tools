import scrapy

class PeMS_Spider(scrapy.Spider):
    name = "pems"
    allowed_domains = "pems.dot.ca.gov"
    start_urls = [http://pems.dot.ca.gov/]

def parse(self, response):
    filename = response.url.split("/")[-2] + '.html'
    with open(filename, 'wb') as f:
        f.write(response.body)