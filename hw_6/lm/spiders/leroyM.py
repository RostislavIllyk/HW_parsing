# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.join(sys.path[0], 'C:\\Users\\rost_\\hw_6'))



import scrapy
from scrapy.http import HtmlResponse
from scrapy.loader import ItemLoader
from lm.items import LmItem


class LeroymSpider(scrapy.Spider):
    name = 'leroyM'
    allowed_domains = ['leroymerlin.ru']
    start_urls = ['http://leroymerlin.ru/']

    def __init__(self, subject):
        self.start_urls = ['https://leroymerlin.ru/search/?q='+subject]
        
    def parse(self, response):
        item_links = response.xpath('//div[@class="product-name"]/a/@href').extract()
        item_next_page = response.xpath('//div[@class="next-paginator-button-wrapper"]/a/@href').extract_first()
        for link in item_links:
            yield response.follow(link, callback=self.item_parse)
        yield response.follow(item_next_page, callback = self.parse)

    def item_parse(self, response:HtmlResponse):
        loader = ItemLoader(item=LmItem(), response=response)
        loader.add_value('ref',response.url)
        loader.add_xpath('title', '//h1/text()')
        loader.add_xpath('price', '//span[@slot="price"]/text()')
        loader.add_xpath('item_var', '//uc-pdp-section-vlimited[@class="section__vlimit"]//ul/li/text()')
        loader.add_xpath('item_var_alt', '//dt[@class="def-list__term"]/text()')
        loader.add_xpath('item_var_alt_values', '//dd/text()')
        loader.add_xpath('pics', '//picture//source[contains(@media,"1024")]/@srcset')
        yield loader.load_item()
        









