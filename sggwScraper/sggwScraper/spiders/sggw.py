import scrapy
from scrapy_playwright.page import PageMethod

from sggwScraper.items import ScientistItem, organizationItem, publicationItem

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
                PageMethod('wait_for_selector', 'div#entitiesT_content ul.ui-dataview-list-container'),
                PageMethod('wait_for_selector', 'div#searchResultsFiltersInnerPanel' )
                ],
            errback=self.errback
        ))

        #redirect to Publications category
        yield response.follow(categories['Publications'], callback=self.parse_publications_links,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'ul.ui-dataview-list-container li.ui-dataview-row')
                ],
            errback=self.errback
        ))
        

    async def parse_scientist_links(self, response):
        page = response.meta['playwright_page']
       
        bw_url='https://bw.sggw.edu.pl'
        total_pages=2#int(response.css('span.entitiesDataListTotalPages::text').get())
        #get scientist links from the page
        current_page=1
        while current_page<=total_pages:
            authors_links=response.css('a.authorNameLink::attr(href)').getall()
            #redirect to every scientist profile
            for author in authors_links:
                yield response.follow(bw_url+author, callback=self.parse_scientist,
                    meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_page_methods =[
                        PageMethod('wait_for_selector', 'div#authorProfileBasicInfoPanel'),
                        PageMethod('wait_for_selector', 'div#authorProfileGridContainer'),
                        PageMethod('wait_for_selector', 'div.otherInfoContainer' )
                        ],
                    errback=self.errback
                ))
            #next page button clicker
            if current_page<total_pages:
                next_button_selector = ".ui-paginator-next.ui-state-default.ui-corner-all"
                await page.wait_for_selector(next_button_selector, state="visible")
                await page.click(next_button_selector)
                await page.wait_for_selector('div#entitiesT_content ul.ui-dataview-list-container')
                content = await page.content()
                response = response.replace(body=content)
                current_page+=1
            else:
                break
                
        
            
        #filters section
        domains_disciplines=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li')
        
        for domain in domains_disciplines:
            organization=organizationItem()
            d_name=domain.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['name']=d_name
            yield organization

        await page.close()
        
        


    async def parse_scientist(self, response):
        '''
            Scrapes scientist profile page
        '''
        page = response.meta['playwright_page']
        
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
        await page.close()
        

        
    async def parse_publications_links(self, response):
        page = response.meta['playwright_page']
        
        bw_url='https://bw.sggw.edu.pl'
        total_pages=10#int(response.css('span.entitiesDataListTotalPages::text').get().replace(',',''))

        current_page=1
        while current_page<=total_pages:
            publications_urls=response.css('div.entity-row-heading-wrapper>h5>a::attr(href)').getall()

            for pub in publications_urls:
                yield response.follow(bw_url+pub, callback=self.parse_publication,
                    meta=dict(
                    playwright=True,
                    playwright_include_page=True,
                    playwright_page_methods =[
                        PageMethod('wait_for_selector', 'div.publicationShortInfo'),
                        PageMethod('wait_for_selector', 'dl.table2ColsContainer dt' )
                        ],
                    errback=self.errback
                ))
            
            #next page button clicker
            if current_page<total_pages:
                next_button_selector = ".ui-paginator-next.ui-state-default.ui-corner-all"
                await page.wait_for_selector(next_button_selector, state="visible")
                await page.click(next_button_selector)
                await page.wait_for_selector('ul.ui-dataview-list-container li.ui-dataview-row')
                content = await page.content()
                response = response.replace(body=content)
                current_page+=1
            else:
                break
        await page.close()


    async def parse_publication(self, response):
        page = response.meta['playwright_page']

        authors_selector = response.css('div.authorList div.authorListElement a')
        authors=[]

        for author in authors_selector:
            authors.append(author.css('span.authorSimple>span::text').getall())

        publication=publicationItem()

        publication['title']=response.css('div.publicationShortInfo>h2::text').get()
        publication['journal']=response.xpath('//*[@id="j_id_3_1q_1_1_1a_5_3_1_1:0:j_id_3_1q_1_1_1a_5_3_1_5_1"]/text()').get()
        publication['publication_date']= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Issue year")]]/following-sibling::dd[1]//text()').get()
        publication['citations_count']=response.xpath('//dt[span/text()="Publication indicators"]/following-sibling::dd[1]/ul/li[1]/a/text()').re_first(r'=\s*(\d+)')
        publication['authors']=authors

        yield publication
        await page.close()
            
    
        
    async def errback(self, failure):
        page = failure.request.meta['playwright_page']
        await page.close()