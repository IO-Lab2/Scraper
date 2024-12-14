import subprocess
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from sggwScraper.spiders.publications import PublicationsSpider
from sggwScraper.spiders.pub1 import Pub1Spider
from sggwScraper.spiders.pub2 import Pub2Spider
from sggwScraper.spiders.pub3 import Pub3Spider
from sggwScraper.spiders.pub4 import Pub4Spider
from sggwScraper.spiders.pub5 import Pub5Spider






subprocess.run(["scrapy", "crawl", "sggw"])


settings = get_project_settings()
process = CrawlerProcess(settings)
process.crawl(PublicationsSpider)
#process.crawl(Pub1Spider)
#process.crawl(Pub2Spider)
#process.crawl(Pub3Spider)
#process.crawl(Pub4Spider)
#process.crawl(Pub5Spider)
process.start()