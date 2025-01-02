import subprocess
#from scrapy.crawler import CrawlerProcess
#from scrapy.utils.project import get_project_settings

#from sggwScraper.spiders.publications import PublicationsSpider






subprocess.run(["scrapy", "crawl", "sggw"])
subprocess.run(["scrapy", "crawl", "publications"])



#settings = get_project_settings()
#process = CrawlerProcess(settings)
#process.crawl(PublicationsSpider)
#process.start()