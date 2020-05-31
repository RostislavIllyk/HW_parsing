# -*- coding: utf-8 -*
import scrapy
from scrapy.http import HtmlResponse
#from pprint import pprint
from book_parser.items import BookParserItem


class Book24ruSpider(scrapy.Spider):
    name = 'book24ru'
    allowed_domains = ['book24.ru']
    
    def __init__(self, subject):
        self.start_urls = ['https://book24.ru/search/?q='+subject]
    def parse(self, response:HtmlResponse):
        next_page_ref = response.xpath("//a[@class='catalog-pagination__item _text js-pagination-catalog-item']//@href").extract_first()
        book_links = response.xpath('//a[@class="book__image-link js-item-element ddl_product_link"]//@href').extract()
        for link in book_links:
            yield response.follow(link, callback=self.book_parse)
        yield response.follow(next_page_ref, callback = self.parse)
    
    
    def book_parse(self, response:HtmlResponse):

        ref=str(response)[5:-1]
        title       = None
        ac_price    = None
        old_price   = None
        is_present  = None
        rating      = None
        author      = None
        title = response.xpath('//h1/text()').extract_first()
        ac_price = response.xpath('//div[@class="item-actions__price"]/b/text()').extract_first()
        old_price = response.xpath('//div[@class="item-actions__price-old"]/text()').extract_first()
        is_present = response.xpath('//span[@class="availability__text"]/text()').extract_first()
        rating = response.xpath('//span[@class="rating__rate-value"]/text()').extract_first()
        author = response.xpath('//a[@class="item-tab__chars-link js-data-link"]/text()').extract_first()
        
        
        yield BookParserItem(ref=ref, title=title, ac_price=ac_price, old_price=old_price, is_present=is_present, rating=rating, author=author)

























































