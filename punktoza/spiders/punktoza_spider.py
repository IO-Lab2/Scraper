import scrapy
from scrapy_playwright.page import PageMethod

class PunktozaSpiderSpider(scrapy.Spider):
    name = "punktoza_spider"
    
    def start_requests(self):
        '''
            Uses scrapy-playwright plugin for Scrapy to handle javascript heavy website.
            Starts scraping when full table has rendered
        '''
        yield scrapy.Request(url='https://punktoza.pl/',
            meta=dict(
                playwright = True,
                playwright_include_page = True,
                playwright_page_methods = [
                    PageMethod('wait_for_selector', "table#DataTables_Table_0 tbody tr.even")
                ]
            ),
            callback=self.parse)

    async def parse(self, response):
        # Parses website's content with playwright's page methods and loops through data table.
        # Yields scraped data       
        page = response.meta['playwright_page']
        page_counter = int(response.css("ul.pagination li.paginate_button.page-item a[data-dt-idx='7']::text").get())

        for i in range(page_counter):
            # Check if table's page is filled with articles
            section_name = response.css("tr.dtrg-group.dtrg-start.dtrg-level-0 th::text").get()
            if section_name != 'Czasopismo': 
                continue
            
            # Loop over rows in data table
            table_rows = response.css("table#DataTables_Table_0 tbody tr.even, table#DataTables_Table_0 tbody tr.odd")
            for row in table_rows:
                publication = row.css('td.dt-head-center.dt-head-nowrap.dtr-control a::text').get()
                if_points = row.css("td.dt-right.dt-head-center.dt-head-nowrap::text").get() + row.css('td.dt-right.dt-head-center.dt-head-nowrap span::text').get()

                # Store and yield scraped data
                info_dict = {
                    'Publication_name': publication,
                    'IF_points': if_points
                }

                yield info_dict
            # Click pagination's next page button
            await page.click("li.paginate_button.page-item.next a[data-dt-idx='8']")

            # Update content after clicking
            content = await page.content()
            response = response.replace(body=content)

        await page.close()

    async def close_page(self, error):
        # Handles the errors
         page = error.request.meta['playwright_page']
         await page.close()
