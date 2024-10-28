import scrapy
from scrapy_playwright.page import PageMethod

class SggwSpider(scrapy.Spider):
    name = "sggw"

    #allowed_domains = ["bw.sggw.edu.pl"]
    #start_urls = ["https://bw.sggw.edu.pl"]
    def start_requests(self):
        url='https://bw.sggw.edu.pl'
        yield scrapy.Request(url,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'div.startPage'),
                PageMethod('wait_for_timeout', 10000)
                ],
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
        
        yield response.follow(categories['People'], callback=self.parse_authors_links,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'div#entitiesT_content')
                ],
            errback=self.errback
        ))
        

    def parse_authors_links(self, response):
        authors_links=response.css('a.authorNameLink::attr(href)').getall()
        bw_url='https://bw.sggw.edu.pl'

        for author in authors_links:
            yield response.follow(bw_url+author, callback=self.parse_author)


    def parse_author(self, response):
        personal_data=response.css('p.authorNamePanel')
        f_name_title=personal_data.css('span.authorName::text').getall()
        yield {
            'name': f_name_title[0],
            'surname': f_name_title[1],
            'title': f_name_title[2]
        }
        


    
        
    async def errback(self, failure):
        page = failure.request.meta['playwright_page']
        await page.close()