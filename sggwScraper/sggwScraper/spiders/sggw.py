import scrapy


class SggwSpider(scrapy.Spider):
    name = "sggw"
    allowed_domains = ["bw.sggw.edu.pl"]
    start_urls = ["https://bw.sggw.edu.pl"]

    def parse(self, response):
        pass
