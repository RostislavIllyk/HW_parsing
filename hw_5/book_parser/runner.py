# -*- coding: utf-8 -*-
"""
Created on Tue May 26 19:43:18 2020

@author: rost_
"""

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from book_parser import settings
from book_parser.spiders.book24ru import Book24ruSpider
from book_parser.spiders.labirintru import LabirintruSpider

from pymongo import MongoClient


if __name__ == '__main__':
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)
    proccess = CrawlerProcess(settings=crawler_settings)
    subject = 'python'
    proccess.crawl(Book24ruSpider , subject = subject)
    proccess.crawl(LabirintruSpider, subject = subject)
    proccess.start()






    client = MongoClient('localhost', 27017)
    mongo_db = client.books
    collection = mongo_db['book24ru']
    print('in mongo_dbase ', collection.name, ' collection records number are: ', collection.count_documents({}))
    
    collection = mongo_db['labirintru']
    print('in mongo_dbase ', collection.name, ' collection records number are: ', collection.count_documents({}))

#    collection.delete_many({})














