# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BookParserItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    
    _id         = scrapy.Field()
    ref         = scrapy.Field()
    title       = scrapy.Field()
    ac_price    = scrapy.Field()
    old_price   = scrapy.Field()
    is_present  = scrapy.Field()
    rating      = scrapy.Field()
    author      = scrapy.Field()
    
