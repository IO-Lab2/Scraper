# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScientistItem(scrapy.Item):
    first_name = scrapy.Field()
    last_name = scrapy.Field()
    academic_title = scrapy.Field()
    research_area = scrapy.Field()
    email = scrapy.Field()
    profile_url = scrapy.Field()
    college=scrapy.Field()
    h_index_scopus = scrapy.Field()
    h_index_wos = scrapy.Field()
    publication_count = scrapy.Field()
    ministerial_score = scrapy.Field()
    organization= scrapy.Field()

    

class publicationItem(scrapy.Item):
    title = scrapy.Field()
    journal = scrapy.Field()
    publication_date = scrapy.Field()
    authors = scrapy.Field()

class organizationItem(scrapy.Item):
    university = scrapy.Field()
    institute= scrapy.Field()
    cathedras = scrapy.Field()
