import scrapy
import logging
import asyncio
from scrapy_playwright.page import PageMethod
from sggwScraper.items import publicationItem

#logging.getLogger('asyncio').setLevel(logging.CRITICAL)

def should_abort_request(request):
    return (
        request.resource_type in ["image", "stylesheet", "font", "media"]
        or ".jpg" in request.url
    )

class PublicationsSpider(scrapy.Spider):
    name = "publications"
    allowed_domains = ["bw.sggw.edu.pl"]

    custom_settings = {
        'PLAYWRIGHT_ABORT_REQUEST': should_abort_request,

    }

    bw_url='https://bw.sggw.edu.pl'

    def start_requests(self):
        
        yield scrapy.Request(url=self.bw_url, 
            callback=self.parse_pages,
                meta=dict(playwright=True, playwright_include_page=True, playwright_context="pages",))
        
    async def parse_pages(self, response):
        page=response.meta['playwright_page']

        await page.goto('https://bw.sggw.edu.pl/globalResultList.seam?r=publication&tab=PUBLICATION&lang=en')
        await page.content()

        total_pages = await page.evaluate("parseInt(document.querySelector('span.entitiesDataListTotalPages').innerText.replace(',', ''))")
        
        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages+1):
            page_url = f'https://bw.sggw.edu.pl/globalResultList.seam?r=publication&tab=PUBLICATION&lang=en&p=bst&pn={page_number}'
            
            yield scrapy.Request(url=page_url,
                callback=self.parse_publications_links,
                meta=dict(
                    playwright=True, 
                    playwright_include_page=True,
                    playwright_context="pages",
                    publication_delay=True,
                    errback=self.errback
                ))
            await asyncio.sleep(1)

        await page.close()

    async def parse_publications_links(self, response):
        try:
            page = response.meta['playwright_page']
            await page.wait_for_selector('div.entity-row-heading-wrapper h5 a', state='visible')
            publications_urls=response.css('div.entity-row-heading-wrapper>h5>a::attr(href)').getall()

            for pub in publications_urls:
                yield scrapy.Request(self.bw_url+pub, callback=self.parse_publication)
            await page.close()

        except Exception as e:
            print(f'Error in parse_publications_links, {e} {response.url}')
            yield scrapy.Request(response.url, callback=self.parse_publications_links, 
                    dont_filter=True,
                    meta=dict(
                        playwright=True, 
                        playwright_include_page=True,
                        playwright_context="pages",
                        errback=self.errback
                    ))
    
    def parse_publication(self, response):
        
        try:
            
            authors_selector = response.css('div.authorListElement>a::attr(href)').getall() or None
            if authors_selector:
                authors_selector=[self.bw_url+link for link in authors_selector]
            
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

            if authors_selector and publication['title']:
                yield publication
        except Exception as e:
            self.logger.error(f"Failed to process {response.url}: {str(e)}")
            print(f'Error in parsing publication, {e} {response.url}')
        
            
    async def errback(self, failure):
        
        self.logger.error(f"Request failed: {repr(failure)}")
        page = failure.request.meta.get('playwright_page')
        if page:
            await page.close()
