from operator import ne
import scrapy
from scrapy_playwright.page import PageMethod

from sggwScraper.items import ScientistItem

class SggwSpider(scrapy.Spider):
    name = "sggw"

    allowed_domains = ["bw.sggw.edu.pl"]
    start_urls = ["https://bw.sggw.edu.pl"]
   

    def parse(self, response):
        disciplines=response.css('a.omega-discipline::text').getall()

        for disc in disciplines:
            yield {
                'discipline': disc
            }
        
        bw_url='https://bw.sggw.edu.pl'

        categories_links=response.css('a.global-stats-link::attr(href)').getall()
        categories_names=response.css('span.global-stats-description::text').getall()
        categories={name: bw_url+link for name, link in zip(categories_names, categories_links)}
        
        yield response.follow(categories['People'], callback=self.parse_scientist_links,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'div#entitiesT_content')
                ],
            errback=self.errback
        ))
        

    async def parse_scientist_links(self, response):
        page = response.meta['playwright_page']
        await page.close()
        authors_links=response.css('a.authorNameLink::attr(href)').getall()
        bw_url='https://bw.sggw.edu.pl'

        for author in authors_links:
            yield response.follow(bw_url+author, callback=self.parse_scientist,
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_page_methods =[
                    PageMethod('wait_for_selector', 'div#infoPageContainer')
                    ],
                errback=self.errback
            ))
        #need fix
        next_page_btn_hidden=response.css('ul#entitiesT_paginator_bottom>li:nth-child(4)::attr(aria-hidden)').get()
        if next_page_btn_hidden!='true':
            yield response.follow(response.url, callback=self.parse_scientist_links,
                meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_page_methods=[
                        PageMethod('wait_for_selector', 'ul#entitiesT_paginator_bottom'),
                        #PageMethod('hover', 'a.ui-paginator-next'),
                        PageMethod('click', 'ul#entitiesT_paginator_bottom a.ui-paginator-next'),
                        PageMethod('wait_for_selector', 'div#entitiesT_content')
                    ],
                    errback=self.errback
                ))
        


    async def parse_scientist(self, response):
        page = response.meta['playwright_page']
        await page.close()
        personal_data=response.css('div.authorProfileBasicInfoPanel')
        name_title=personal_data.css('span.authorName::text').getall()
        research_area=response.css('div.researchFieldsPanel')

        scientist=ScientistItem()
        
        scientist['first_name']= name_title[0]
        scientist['last_name']= name_title[1]
        scientist['academic_title']= name_title[2]
        scientist['research_area']= research_area.css('ul.ul-element-wcag li span::text').getall()
        scientist['email']= personal_data.css('p.authorContactInfoEmailContainer>a::text').get()
        scientist['profile_url']= response.url
        scientist['h_index']=response.css('div.bibliometricsPanel>ul.ul-element-wcag>li:nth-child(1)>a::text').get()
        scientist['publication_count']=response.css('div.achievementsTable ul.ul-element-wcag>li:nth-child(1)>div.achievmentResultListLink>a::text').get()
        #need fix
        #scientist['ministerial_score']=response.css('div.bibliometricsPanel>ul.ul-element-wcag>li:nth-child(6)>div::text').get()
        
        yield scientist
        


    
        
    async def errback(self, failure):
        page = failure.request.meta['playwright_page']
        await page.close()