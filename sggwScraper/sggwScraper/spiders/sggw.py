import scrapy
import asyncio
import logging
import json
from scrapy_playwright.page import PageMethod
from sggwScraper.items import ScientistItem, organizationItem, publicationItem

logging.getLogger('asyncio').setLevel(logging.CRITICAL)

def should_abort_request(request):
    return (
        request.resource_type in ["image", "stylesheet", "font", "media"]
        or ".jpg" in request.url
    )


class SggwSpider(scrapy.Spider):
    name = "sggw"

    allowed_domains = ["bw.sggw.edu.pl"]
    start_urls = ["https://bw.sggw.edu.pl"]

    custom_settings = {
        'PLAYWRIGHT_ABORT_REQUEST': should_abort_request,
        'AUTOTHROTTLE_START_DELAY': 0.5,
        'AUTOTHROTTLE_MAX_DELAY': 60,
        # The average number of requests Scrapy should be sending in parallel to
        # each remote server
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 4,
        # Enable showing throttling stats for every response received:
        'AUTOTHROTTLE_DEBUG': False
    }

    bw_url='https://bw.sggw.edu.pl'
    
    def parse(self, response):
        #disciplines=response.css('a.omega-discipline::text').getall()

        categories_links=response.css('a.global-stats-link::attr(href)').getall()
        categories_names=response.css('span.global-stats-description::text').getall()
        categories={name: self.bw_url+link for name, link in zip(categories_names, categories_links)}

        #redirect to People category
        yield scrapy.Request(categories['People'], callback=self.parse_people_page,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'a.authorNameLink'),
                PageMethod('wait_for_selector', 'div#searchResultsFiltersInnerPanel' ),
                PageMethod('wait_for_selector', 'div#afftreemain div#groupingPanel ul.ui-tree-container'),
                PageMethod("evaluate", """
                            async () => {
                                const expandAllNodes = async () => {
                                    const buttons = Array.from(document.querySelectorAll('.ui-tree-toggler'));
                                    for (const button of buttons) {
                                        if (button.getAttribute('aria-expanded') === 'false') {
                                            await button.click();
                                            await new Promise(r => setTimeout(r, 300));  // timeout to wait for the animation
                                        }
                                    }
                                };
                                await expandAllNodes();
                            }
                        """)
                ],
            errback=self.errback
        ))
        
    async def parse_people_page(self, response):
        page=response.meta['playwright_page']
        #filters section
        organizations=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>ul.ui-treenode-children>li')
        university=response.css('div#afftreemain>div#groupingPanel>ul.ui-tree-container>li>div.ui-treenode-content div.ui-treenode-label>span>span::text').get()
        for org in organizations:
            organization=organizationItem()
            organization['university']=university

            institute=org.css('div.ui-treenode-content div.ui-treenode-label span>span::text').get()
            organization['institute']=institute

            cathedras = org.css('ul.ui-treenode-children li.ui-treenode-leaf div.ui-treenode-content div.ui-treenode-label span>span::text').getall()
            if cathedras:
                organization['cathedras']=cathedras
            else:
                organization['cathedras']=[]
            
            yield organization

        
        total_pages=1#int(response.css('span.entitiesDataListTotalPages::text').get())

        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages + 1):
            page_url = f'https://bw.sggw.edu.pl/globalResultList.seam?q=&oa=false&r=author&tab=PEOPLE&conversationPropagation=begin&lang=en&qp=openAccess%3Dfalse&p=xyz&pn={page_number}'
            yield scrapy.Request(url=page_url,
                callback=self.parse_scientist_links, dont_filter=True,
                meta=dict(
                    playwright=True, 
                    playwright_include_page=True,
                    playwright_page_methods=[
                        PageMethod('wait_for_selector', 'a.authorNameLink', state='visible')
                        ],
                    errback=self.errback
            ))
        await page.close()
        
        
    async def parse_scientist_links(self, response):
        page=response.meta['playwright_page']
        
        authors_links=response.css('a.authorNameLink::attr(href)').getall()
        #redirect to every scientist profile
        for author in authors_links:
            yield scrapy.Request(self.bw_url+author, callback=self.parse_scientist, dont_filter=True,
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_context="people",
                errback=self.errback
            ))
        await page.close()
        
    async def parse_scientist(self, response):
        '''
            Scrapes scientist profile page
        '''
        page = response.meta['playwright_page']

        scientist=ScientistItem()

        try:
            personal_data=response.css('div.authorProfileBasicInfoPanel')

            names=personal_data.css('p.author-profile__name-panel::text').get()
            
            if names:
                names=names.split()
                scientist['first_name']= names[0]
                scientist['last_name']= names[1]

            scientist['academic_title']=personal_data.css('p.author-profile__name-panel span:nth-of-type(2)::text').get() or None
            
            
            
            
            scientist['email']= personal_data.css('dd[property="email"] a::text').get() or None
            

            scientist['profile_url']= response.url
            scientist['position']=personal_data.css('p.possitionInfo span::text').get() or None

            scientist['h_index_scopus']=response.xpath('//li[@class="hIndexItem"][span[contains(text(), "Scopus")]]/a[@class="indicatorValue"]/text()').get() or 0
            print(scientist['h_index_scopus'])
            scientist['h_index_wos']=response.xpath('//li[@class="hIndexItem"][span[contains(text(), "WoS")]]//a/text()').get() or 0
            
            pub_count=response.xpath('//li[contains(@class, "li-element-wcag")][span[@class="achievementName" and contains(text(), "Publications")]]//a/text()').get()
            scientist['publication_count']=pub_count or 0

            if response.css('ul.bibliometric-data-list li>span.indicatorName'):
                #loading spinner ui-outputpanel-loading ui-widget

                await page.wait_for_function(
                                    """() => {
                                        const element = document.querySelector('div#j_id_3_1q_1_1_8_6n_a_2');
                                        return element && element.textContent.trim().length > 0;
                                    }"""
                                )
                ministerial_score= await page.evaluate('document.querySelector("div#j_id_3_1q_1_1_8_6n_a_2")?.textContent.trim()')
                    
                if '—' not in ministerial_score:
                    scientist['ministerial_score']=ministerial_score
                else:
                    scientist['ministerial_score']=0
            else:
                scientist['ministerial_score']=0

            organization_scientist=personal_data.css('ul.authorAffilList li span a>span::text').getall() or None
            if organization_scientist:
                scientist['organization']=organization_scientist


            research_area=response.css('div.researchFieldsPanel ul.ul-element-wcag li span::text').getall()
            scientist['research_area']=research_area or None

            if scientist['research_area'] and scientist['academic_title']:
                yield scientist

        except Exception as e:
            self.logger.error(f'Error in parse_scientist, {e} {response.url}')
            yield scrapy.Request(response.url, callback=self.parse_scientist, dont_filter=True,
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                errback=self.errback
            ))
        finally:
            await page.close()

        
    
            
    async def errback(self, failure):
        
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()