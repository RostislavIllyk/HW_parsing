# -*- coding: utf-8 -*
import scrapy
from scrapy.http import HtmlResponse
#from pprint import pprint
from book_parser.items import BookParserItem


class LabirintruSpider(scrapy.Spider):
    name = 'labirintru'
    allowed_domains = ['labirint.ru']
    
    def __init__(self, subject):
        self.start_urls = ['https://www.labirint.ru/search/'+subject+'/']
        self.subject = subject
        
    def parse(self, response:HtmlResponse):

        book_links = response.xpath('//a[@class="cover"]//@href').extract()
        for link in book_links:
            yield response.follow(link, callback=self.book_parse)
            
        next_page_ref = response.xpath('//a[@class="pagination-next__text"]//@href').extract_first()
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
        ac_price = response.xpath('//span[@class="buying-pricenew-val-number"]/text()').extract_first()
        old_price = response.xpath('//span[@class="buying-priceold-val-number"]/text()').extract_first()
        is_present = response.xpath('//div[@class="prodtitle-availibility rang-available"]/span/text()').extract_first()
        rating = response.xpath('//div[@id="rate"]/text()').extract_first()
        author=None
        
        yield BookParserItem(ref=ref, title=title, ac_price=ac_price, old_price=old_price, is_present=is_present, rating=rating, author=author)
















































