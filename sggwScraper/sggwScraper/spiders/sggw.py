import scrapy
import playwright
import asyncio
import logging
from scrapy_playwright.page import PageMethod
from playwright.async_api import TimeoutError
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

        #redirect to Publications category
        yield scrapy.Request(categories['Publications'], callback=self.parse_publication_page,
        meta=dict(
            playwright=True,
            playwright_include_page=True,
            playwright_page_methods =[
                PageMethod('wait_for_selector', 'span.entitiesDataListTotalPages', state='visible')
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

        
        total_pages=0#int(response.css('span.entitiesDataListTotalPages::text').get())

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

            name_title=personal_data.css('span.authorName::text').getall()
            scientist['first_name']= name_title[0]
            scientist['last_name']= name_title[1]
            academic_title=None
            if len(name_title)>2:
                academic_title=name_title[2]
                scientist['academic_title']= academic_title
            
            research_area=response.css('div.researchFieldsPanel ul.ul-element-wcag li span::text').getall()
            scientist['research_area']=research_area or None
            
            
            scientist['email']= personal_data.css('p.authorContactInfoEmailContainer>a::text').get() or None
            

            scientist['profile_url']= response.url
            scientist['college']='SGGW'

            scientist['h_index_scopus']=response.xpath('//li[@class="hIndexItem"][span[contains(text(), "Scopus")]]//a/text()').get() or 0
            
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
                scientist['organization']=organization_scientist[0]

            if research_area and academic_title:
                yield scientist

        except Exception:
            yield scrapy.Request(response.url, callback=self.parse_scientist, dont_filter=True,
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                errback=self.errback
            ))
        finally:
            
            await asyncio.sleep(0.2)
            await page.close()

        
    async def parse_publication_page(self, response):
        page = response.meta['playwright_page']
        total_pages = 0#int(response.css('span.entitiesDataListTotalPages::text').get().replace(',', ''))
        
        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages + 1):
            page_url = f'https://bw.sggw.edu.pl/globalResultList.seam?r=publication&tab=PUBLICATION&lang=en&p=hgu&pn={page_number}'
            yield scrapy.Request(url=page_url,
                callback=self.parse_publications_links,
                meta=dict(
                    playwright=True, 
                    playwright_include_page=True,
                    playwright_page_methods=[
                        PageMethod('wait_for_selector', 'div.entity-row-heading-wrapper h5 a')
                        ],
                    errback=self.errback
            ))
        
        await page.close()
        
    async def parse_publications_links(self, response):
        page = response.meta['playwright_page']
        

        publications_urls=response.css('div.entity-row-heading-wrapper>h5>a::attr(href)').getall()

        for pub in publications_urls:
            yield scrapy.Request(self.bw_url+pub, callback=self.parse_publication,
                meta=dict(
                playwright=True,
                playwright_include_page=True,
                playwright_context="publication",
                playwright_page_methods =[
                    PageMethod('wait_for_selector', 'div.publicationShortInfo', state='visible'),
                    PageMethod('wait_for_selector', 'dl.table2ColsContainer dt', state='visible'),
                    PageMethod('wait_for_selector', 'dl.table2ColsContainer dd', state='visible')
                    ],
                errback=self.errback
                ))
        await page.close()
    
    async def parse_publication(self, response):
        page = response.meta['playwright_page']
        
        try:
            
            authors_selector = response.css('div.authorListElement>a::attr(href)').getall() or None
            if authors_selector:
                authors_selector=[self.bw_url+link for link in authors_selector]
            '''
            if authors_selector:
                for author in authors_selector:
                    authors.append(author.css('span.authorSimple>span::text').getall())
            '''
            publication=publicationItem()

            
            
            publication['title']=response.css('div.publicationShortInfo>h2::text').get() or None


            #journal=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal")]]/following-sibling::dd[1]//a/text()').get()
            #journal2=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal")]]/following-sibling::dd[1]//div/text()').get()
            #journal3=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal")]]/following-sibling::dd[1]/text()').get()
            publisher=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]//a/span/span/text()').get()
            publisher2=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]//div/text()').get()
            publisher3=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]/text()').get()

            pub=[publisher, publisher2, publisher3]
            publishers=[j for j in pub if j and j.strip()!='']   
            publication['publisher']=publishers[0] if publishers else None
            
            publication_date= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Year of creation")]]/following-sibling::dd[1]/text()').get()
            publication_date_div= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Year of creation")]]/following-sibling::dd[1]/div/text()').get()
            publication_date2= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Issue year")]]/following-sibling::dd[1]/text()').get()
            publication_date_div2= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Issue year")]]/following-sibling::dd[1]/div/text()').get()
            
            pub_dates=[publication_date, publication_date2, publication_date_div, publication_date_div2]
            valid_dates=[date for date in pub_dates if date and date.strip()!='0' and date.strip()!='']
            publication['publication_date'] = valid_dates[0] if valid_dates else None
            
            publication['authors']=authors_selector

            vol=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Vol")]]/following-sibling::dd[1]/div/text()').get()
            edition=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Edition")]]/following-sibling::dd[1]/text()').get()
            parts=[vol, edition]
            publication['vol']= parts[0] if parts else None

            if authors_selector:
                yield publication
        except Exception as e:
            self.logger.error(f"Failed to process {response.url}: {str(e)}")
            print(f'Error in parsing publication, {e} {response.url}')
        finally:
            
            await asyncio.sleep(0.2)
            await page.close()
            
    async def errback(self, failure):
        
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()