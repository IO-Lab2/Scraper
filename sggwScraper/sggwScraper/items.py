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
