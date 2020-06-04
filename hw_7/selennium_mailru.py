# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 13:24:13 2020

@author: Rb
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

import time
from pprint import pprint
import hashlib

from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from pymongo import MongoClient



class mailru_messages():

    chrome_options=Options()
    chrome_options.add_argument('start-maximized')    
    driver = webdriver.Chrome(options=chrome_options)
    driver.get('https://www.mail.ru/')
    messages=[]

    client=MongoClient('localhost', 27017)
    db = client['mailru']
    mailru_content = db.mailru_content
    
    
    
    def get_hash_id(cls, data):
            data_str=str(data)
            data_bin = data_str.encode('utf-8')
            data_hash = hashlib.md5(data_bin)
            _id = data_hash.hexdigest()
            return _id
    
    
    
    def login_to_mailru(cls):
    
        #                    Данная конструкция срабатывала через раз   !
        try:
            form_elem = WebDriverWait(cls.driver, 150).until(EC.presence_of_element_located((By.ID, 'mailbox:login')))
            print("Page is ready!")
        except TimeoutException:
            print( "Loading took too much time!")
        
        time.sleep(3)
        form_elem=cls.driver.find_element_by_id('mailbox:login')
        form_elem.send_keys('study.ai_172')
        
        time.sleep(1)
        form_elem.send_keys(Keys.RETURN)
    
        try:
            form_elem = WebDriverWait(cls.driver, 150).until(EC.presence_of_element_located((By.ID, 'mailbox:password')))
            print("Page is ready!")
        except TimeoutException:
            print( "Loading took too much time!")
        
        time.sleep(5)
        form_elem=cls.driver.find_element_by_id('mailbox:password')
        form_elem.send_keys('NextPassword172')
        form_elem.send_keys(Keys.RETURN)

    
    
    def get_links_to_messages(cls):
        list_to_pars=[]
        for j in range(3):
            try:
                items_description = WebDriverWait(cls.driver, 150).until(EC.presence_of_all_elements_located(
                        (By.XPATH, '//a[@class="llc js-tooltip-direction_letter-bottom js-letter-list-item llc_pony-mode llc_normal"]')))
                print("Page is ready!")
            except TimeoutException:
                print( "Loading took too much time!")
#            time.sleep(3)
            items_description = cls.driver.find_elements_by_xpath('//a[@class="llc js-tooltip-direction_letter-bottom js-letter-list-item llc_pony-mode llc_normal"]')
            for i in items_description:
                items_ref = i.get_attribute("href")
                list_to_pars.append(items_ref)
            set_to_pars = set(list_to_pars)
            list_to_pars = list(set_to_pars)
            items_description[-1].send_keys(Keys.PAGE_DOWN)
            print(j)
        print(len(list_to_pars))
        return list_to_pars


    
    def get_info_from_message(cls, m):
        item={}
        letter_text=[]
        cls.driver.get(m)
        
        time.sleep(7)
        blocks = cls.driver.find_elements_by_xpath('//tr//tbody')
        for block in blocks:
            text = block.text
            letter_text.append(text)
            print(text)
        item['_id'] = cls.get_hash_id(m)
        item['letter_text'] = letter_text
        item['author'] = cls.driver.find_elements_by_xpath('//span[@class="letter-contact"]')[0].text
        item['date'] = cls.driver.find_elements_by_xpath('//div[@class="letter__date"]')[0].text
        item['title'] = cls.driver.find_elements_by_xpath('//h2')[0].text
        item['ref']= m
        cls.messages.append(item)
        return item




    def put_info_into_db(cls):
        print('in mongo_dbase records number are: ', cls.mailru_content.count_documents({}))
        count=0
        for message in collector.messages:
            try:
                cls.mailru_content.insert_one(message)
                count=count+1
            except:
                pass
        print('success write records count', count)
        print('in mongo_dbase records number are: ', cls.mailru_content.count_documents({}))






if __name__=="__main__":
    
    collector = mailru_messages()
    collector.login_to_mailru()
#    time.sleep(60)
    list_to_pars = collector.get_links_to_messages()
    
    for m in list_to_pars:
        item = collector.get_info_from_message(m)
        pprint(item)
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print()
        time.sleep(3)
    collector.put_info_into_db()
    
    
    


