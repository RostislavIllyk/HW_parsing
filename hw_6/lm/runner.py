# -*- coding: utf-8 -*-
"""
Created on Sat May 30 19:25:38 2020

@author: rost_
"""

import sys
import os
sys.path.append(os.path.join(sys.path[0], 'C:\\Users\\rost_\\hw_6'))



from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from lm import settings
from lm.spiders.leroyM import LeroymSpider

from pymongo import MongoClient


if __name__ == '__main__':
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)
    proccess = CrawlerProcess(settings=crawler_settings)
    subject = 'гвозди'
    subject = 'фанера'
    proccess.crawl(LeroymSpider , subject = subject)
    proccess.start()



    client = MongoClient('localhost', 27017)
    mongo_db = client.nails
    collection = mongo_db['leroyM']
    print('in mongo_dbase ', collection.name, ' collection records number are: ', collection.count_documents({}))
    

#    collection.delete_many({})




