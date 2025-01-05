import scrapy
from bs4 import BeautifulSoup as bs
from lxml import etree
from sggwScraper.items import publicationItem



class PublicationsSpider(scrapy.Spider):
    name = "publications"
    allowed_domains = ["bw.sggw.edu.pl"]

    headers = {
            "Accept": "application/xml, text/xml, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Faces-Request": "partial/ajax",
            "Host": "bw.sggw.edu.pl",
            "Origin": "https://bw.sggw.edu.pl",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
    
    formdata_pages = {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "resultTabsOutputPanel",
            "primefaces.ignoreautoupdate": "true",
            "javax.faces.partial.execute": "resultTabsOutputPanel",
            "javax.faces.partial.render": "resultTabsOutputPanel",
            "resultTabsOutputPanel": "resultTabsOutputPanel",
            "resultTabsOutputPanel_load": "true",
        }

    bw_url='https://bw.sggw.edu.pl'

    def start_requests(self):
        
        url='https://bw.sggw.edu.pl/globalResultList.seam?r=publication&tab=PUBLICATION&lang=en'
        yield scrapy.FormRequest(url=url,
                callback=self.parse_pages, 
                headers=self.headers,
                formdata=self.formdata_pages)
        
    def parse_pages(self, response):
        response_bytes = response.body
        root = etree.fromstring(response_bytes)
        cdata_content = root.xpath('//update/text()')[0]
        soup = bs(cdata_content, 'html.parser')
        total_pages=int(soup.find('span', class_='entitiesDataListTotalPages').text.replace(',', ''))
        
        #Generate requests for each page based on the total number of pages
        for page_number in range(1, total_pages+1):
            page_url = f'https://bw.sggw.edu.pl/globalResultList.seam?r=publication&tab=PUBLICATION&lang=en&p=bst&pn={page_number}'

            yield scrapy.FormRequest(url=page_url,
                callback=self.parse_publications_links, 
                headers=self.headers,
                formdata=self.formdata_pages)


    def parse_publications_links(self, response):
        response_bytes = response.body
        root = etree.fromstring(response_bytes)
        cdata_content = root.xpath('//update/text()')[0]
        soup = bs(cdata_content, 'html.parser')
        links_selectors = soup.find_all('a', class_='infoLink')
        if links_selectors:
            links=[link.get('href') for link in links_selectors]
            
        for link in links:
            yield scrapy.Request(self.bw_url+link, callback=self.parse_publication)
    
    def parse_publication(self, response):
        
        try:
            authors_selector = response.css('div.authorListElement>a::attr(href)').getall() or None
            if authors_selector:
                authors_selector=[self.bw_url+link for link in authors_selector]
            
            publication=publicationItem()

            publication['title']=response.css('div.publicationShortInfo>h2::text').get() or None


            journal=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal series")]]/following-sibling::dd[1]//a/text()').get()
            #journal2=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal")]]/following-sibling::dd[1]//div/text()').get()
            #journal3=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Journal")]]/following-sibling::dd[1]/text()').get()

            #jour=[journal, journal2, journal3]
            #journals=[j for j in jour if j and j.strip()!=''] 
            publication['journal']=journal or None

            publisher=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]//a/span/span/text()').get()
            publisher2=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]//div/text()').get()
            publisher3=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Publisher")]]/following-sibling::dd[1]/text()').get()

            publishers=[publisher, publisher2, publisher3]
            publishers=[j for j in publishers if j and j.strip()!='']   
            publication['publisher']=publishers[0] if publishers else None
            
            publication_date= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Year of creation")]]/following-sibling::dd[1]/text()').get()
            publication_date_div= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Year of creation")]]/following-sibling::dd[1]/div/text()').get()
            publication_date2= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Issue year")]]/following-sibling::dd[1]/text()').get()
            publication_date_div2= response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Issue year")]]/following-sibling::dd[1]/div/text()').get()
            
            pub_dates=[publication_date, publication_date2, publication_date_div, publication_date_div2]
            pub_dates=[date for date in pub_dates if date and date.strip()!='0' and date.strip()!='']
            publication['publication_date'] = pub_dates[0] if pub_dates else None
            
            publication['authors']=authors_selector

            vol=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Vol")]]/following-sibling::dd[1]/text()').get()
            vol_div=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Vol")]]/following-sibling::dd[1]/div/text()').get()

            parts=[vol, vol_div]
            
            parts=[p for p in parts if p and p.strip()!='']
            publication['vol']= parts[0] if parts else None


            ministerial_score=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Score (nominal)")]]/following-sibling::dd[1]/text()').get()
            ministerial_score_div=response.xpath('//dl[contains(@class, "table2ColsContainer")]//dt[span[contains(text(), "Score (nominal)")]]/following-sibling::dd[1]/div/text()').get()

            m_scores=[ministerial_score, ministerial_score_div]
            m_scores=[s for s in m_scores if s and s.strip()!='']
            publication['ministerial_score']=m_scores[0] if m_scores else None

            
        except Exception as e:
            self.logger.error(f"Error in parsing publication {response.url}: {str(e)}")
        finally:
            if publication['authors'] and publication['title']:
                yield publication
        