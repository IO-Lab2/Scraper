import scrapy
from scrapy_playwright.page import PageMethod

from sggwScraper.items import ScientistItem, organizationItem

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

        #redirect to People category
        yield response.follow(categories['People'], callback=self.parse_scientist_links,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'div#searchResultsMainPanel')
                ],
            errback=self.errback
        ))
        

    async def parse_scientist_links(self, response):
        page = response.meta['playwright_page']
        await page.close()
        #get scientist links from the page
        authors_links=response.css('a.authorNameLink::attr(href)').getall()
        bw_url='https://bw.sggw.edu.pl'

        #redirect to every scientist profile
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
        #next page button clicker need fix
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
            
        #filters section
        domains_disciplines=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li')
        
        for domain in domains_disciplines:
            organization=organizationItem()
            d_name=domain.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['name']=d_name
            yield organization
        
        


    async def parse_scientist(self, response):
        '''
            Scrapes scientist profile page
        '''
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
        bw_url='https://bw.sggw.edu.pl'
        publications_url=bw_url+response.css('div#authorTabsPanel>a::attr(href)').getall()[1]
        yield response.follow(publications_url, callback=self.parse_publication_links,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'div.tabContentPanel')
                ],
            errback=self.errback
        ))

        
    async def parse_publication_links(self, response):
        page = response.meta['playwright_page']
        await page.close()
        #need fix
        #publications_links=response.css('h5.entity-row-title>a.infoLink::attr(href)').getall()


    
        
    async def errback(self, failure):
        page = failure.request.meta['playwright_page']
        await page.close()