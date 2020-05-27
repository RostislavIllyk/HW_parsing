# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from pymongo import MongoClient
import hashlib

class BookParserPipeline:
    
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_db = client.books
    
    
    def process_item(self, item, spider):
        
        
        if  spider.name == 'labirintru':
            try:
                sep = item['title'].find(':')
                item['author'] = item['title'][:sep]
                item['title']  = item['title'][sep+1:]
            except:
                pass
        
        
        try:
            item['ac_price']  = int(''.join(filter(str.isdigit, item['ac_price'])))
        except:
            pass
        try:
            item['old_price'] = int(''.join(filter(str.isdigit, item['old_price'])))
        except:
            pass
        try:
            item['rating'] = float(item['rating'].replace(',','.'))
        except:
            pass
        
        book_item_str=str(item['ref'])
        book_item_bin = book_item_str.encode('utf-8')
        book_item_hash = hashlib.md5(book_item_bin)
        item['_id'] = book_item_hash.hexdigest()
        
        
        
        
        
        
        
        collection = self.mongo_db[spider.name]
        collection.insert_one(item)
        return item
