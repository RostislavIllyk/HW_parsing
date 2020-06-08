# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 13:24:13 2020

@author: Rb
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from pprint import pprint
import requests
import os
import hashlib
from pymongo import MongoClient

from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By



class mvideo_hot_items():

    chrome_options=Options()
    chrome_options.add_argument('start-maximized')
    driver = webdriver.Chrome(options=chrome_options)
    driver.get('https://www.mvideo.ru/')
    goods = []
    client=MongoClient('localhost', 27017)
    db = client['mvideoru']
    mv_goods = db.mv_goods


    def get_hash_id(cls, data):
            data_str=str(data)
            data_bin = data_str.encode('utf-8')
            data_hash = hashlib.md5(data_bin)
            _id = data_hash.hexdigest()
            return _id


    def jump(cls):
        try:
            a = WebDriverWait(cls.driver,10).until(
                    EC.presence_of_element_located((By.XPATH,'//div[@class="accessories-carousel-holder carousel tabletSwipe"]//div[@class="carousel-paging"]//a[4]')))
            print("Page is ready!")
        except TimeoutException:
            print( "Loading took too much time!")
        a=cls.driver.find_elements_by_xpath('//div[@class="accessories-carousel-holder carousel tabletSwipe"]//div[@class="carousel-paging"]//a[4]')
        a[0].click()
        time.sleep(1.5)


    def get_items_description(cls):
        items_description = cls.driver.find_elements_by_xpath('//div[@class="gallery-layout"][2]//a[@class="sel-product-tile-title"]')
        pic = cls.driver.find_elements_by_xpath('//div[@class="gallery-layout"][2]//img')    
        return items_description, pic


    def get_item_info(cls, items_description, pic):
        ref = items_description.get_attribute("href")
        pic_ref = pic.get_attribute("src")
        try:
            r=requests.get(pic_ref)
            f=open('./temp/'+ref[-8:]+'.jpg', 'wb')
            f.write(r.content)
            f.close()
        except:
            print('Pic download ERRORR!!!')
        
        info = items_description.get_attribute("data-product-info").replace('\t','')
        info = info.replace('\t','')
        info = info.replace(',','')
        info = info.replace('"','')
        info = info.replace('}','')
        info = info.replace('{','')
        info = info.split('\n')
        
        info_voc=[]
        for m in range(1,len(info)-2):
            try:
                sep = info[m].find(':')
                key = info[m][:sep]
                val = info[m][sep+1:]
                tempo = {key: val}
                info_voc.append(tempo)
            except:
                continue
        item={}
        item['_id'] = cls.get_hash_id(ref)
        item['ref'] = ref
        item['pic_ref'] = pic_ref
        item['info_voc'] = info_voc
        cls.goods.append(item)
        
        return item


    def put_info_into_db(cls):
        print('in mongo_dbase records number are: ', cls.mv_goods.count_documents({}))
        count=0
        for item in search.goods:
            try:
                cls.mv_goods.insert_one(item)
                count=count+1
            except:
                pass
        print('success write records count', count)
        print('in mongo_dbase records number are: ', cls.mv_goods.count_documents({}))






if __name__=='__main__':

    try:
        os.mkdir('./temp')
    except:
        pass
    
    search = mvideo_hot_items()
    search.jump()
    items_description, pic = search.get_items_description()
    for i in range(len(items_description)):
        item = search.get_item_info(items_description[i], pic[i])
        pprint(item)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print()
    search.put_info_into_db()
    
    
    
    
    
    
    
    
    
    
    










