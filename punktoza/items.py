# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JournalItem(scrapy.Item):
    name = scrapy.Field()
    if_points = scrapy.Field()
    publisher = scrapy.Field()
    journal_type = scrapy.Field()