import scrapy
from scrapy_playwright.page import PageMethod

class SggwSpider(scrapy.Spider):
    name = "sggw"

    #allowed_domains = ["bw.sggw.edu.pl"]
    #start_urls = ["https://bw.sggw.edu.pl"]
    def start_requests(self):
        url='https://bw.sggw.edu.pl/index.seam'
        yield scrapy.Request(url,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            errback=self.errback
        ))

    async def parse(self, response):
        page = response.meta['playwright_page']
        await page.close()
        disciplines=response.css('a.omega-discipline::text').getall()

        for disc in disciplines:
            yield {
                'discipline': disc
            }
        
        bw_url='https://bw.sggw.edu.pl'

        categories_links=response.css('a.global-stats-link::attr(href)').getall()
        categories_names=response.css('span.global-stats-description::text').getall()
        categories={name: bw_url+link for name, link in zip(categories_names, categories_links)}

        
        yield {
            'link': categories['People']
        }
        
        
    async def errback(self, failure):
        page = failure.request.meta['playwright_page']
        await page.close()