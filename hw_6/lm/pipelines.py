# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
from scrapy.pipelines.images import ImagesPipeline
import os
import os.path
from scrapy.utils.python import to_bytes

from pymongo import MongoClient
import hashlib


class LmPipeline:
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongo_db = client.nails      
        
    def process_item(self, item, spider):
        try:
            item['price'] = int(item['price'].replace(' ',''))
        except:
            pass
        try:
            id_item_str=str(item['ref'])
            id_item_bin = id_item_str.encode('utf-8')
            id_item_hash = hashlib.md5(id_item_bin)
            item['_id'] = id_item_hash.hexdigest()        
        except:
            item['_id']='Error'
        try:
            if len(item['item_var_alt']) == len(item['item_var_alt_values']):
                for i in range(len(item['item_var_alt'])):
                    item['item_var_alt'][i] = {item['item_var_alt'][i]:item['item_var_alt_values'][i]}       
        except:
            pass
        del item['item_var_alt_values']
        collection = self.mongo_db[spider.name]
        try:
            collection.insert_one(item)    
        except:
            item['item_var'] = [{'ErroR': 'ERROR'}]
            collection.insert_one(item)    
        return item
    
    
    
class Lm_Image_Pipline(ImagesPipeline):
    
    def get_media_requests(self, item, info):
        if item['pics']:
            for img in item['pics']:
                try:
                    yield scrapy.Request(img)
                except Exception as e:
                    print('!!!ERROR!!! : ',e)
        
    def item_completed(self, results, item, info):
        if results:
            item['pics']=[itm[1] for itm in results if itm[0]]
        return item

    def file_path(self, request, response=None, info=None):
        url = request.url
        media_guid = hashlib.sha1(to_bytes(url)).hexdigest()  
        media_ext = os.path.splitext(url)[1]  
        if url[-7]=='_':
            extra=url[-15:-7]
        else:
            extra=url[-12:-4]
        return 'full/'+str(extra)+'/%s%s' % (media_guid, media_ext)








