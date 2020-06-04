# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 13:24:13 2020

@author: Rb
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from pprint import pprint
import hashlib

#from selenium.webdriver.support import expected_conditions as EC
#from selenium.common.exceptions import TimeoutException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.common.by import By
from pymongo import MongoClient

def get_hash_id(data):
        data_str=str(data)
        data_bin = data_str.encode('utf-8')
        data_hash = hashlib.md5(data_bin)
        _id = data_hash.hexdigest()
        return _id

def get_info_from_message(m):
    item={}
    letter_text=[]
    driver.get(m)
    
    time.sleep(7)
    blocks = driver.find_elements_by_xpath('//tr//tbody')
    for block in blocks:
        text = block.text
        letter_text.append(text)
        print(text)
    item['_id'] = get_hash_id(m)
    item['letter_text'] = letter_text
    item['author'] = driver.find_elements_by_xpath('//span[@class="letter-contact"]')[0].text
    item['date'] = driver.find_elements_by_xpath('//div[@class="letter__date"]')[0].text
    item['title'] = driver.find_elements_by_xpath('//h2')[0].text
    item['ref']= m
    return item

def get_links_to_messages():
    list_to_pars=[]
    for j in range(7):
    #    try:
    #        items_description = WebDriverWait(driver, 150).until(EC.presence_of_all_elements_located(
    #                driver.find_element_by_xpath('//a[@class="llc js-tooltip-direction_letter-bottom js-letter-list-item llc_pony-mode llc_normal"]')))
    #        print("Page is ready!")
    #    except TimeoutException:
    #        print( "Loading took too much time!")
        items_description = driver.find_elements_by_xpath('//a[@class="llc js-tooltip-direction_letter-bottom js-letter-list-item llc_pony-mode llc_normal"]')
        for i in items_description:
            items_ref = i.get_attribute("href")
            list_to_pars.append(items_ref)
        set_to_pars = set(list_to_pars)
        list_to_pars = list(set_to_pars)
        items_description[-1].send_keys(Keys.PAGE_DOWN)
        time.sleep(3)
        print(j)
    print(len(list_to_pars))
    return list_to_pars







if __name__=="__main__":

    driver = webdriver.Chrome()
    driver.get('https://www.mail.ru/')
    
    #                    Данная конструкция срабатывала через раз   !
    #try:
    #    form_elem = WebDriverWait(driver, 150).until(EC.presence_of_element_located((By.ID, 'mailbox:login')))
    #    print("Page is ready!")
    #except TimeoutException:
    #    print( "Loading took too much time!")
    
    time.sleep(3)
    form_elem=driver.find_element_by_id('mailbox:login')
    form_elem.send_keys('study.ai_172')
    
    time.sleep(1)
    form_elem.send_keys(Keys.RETURN)
    
    #try:
    #    form_elem = WebDriverWait(driver, 150).until(EC.presence_of_element_located((By.ID, 'mailbox:password')))
    #    print("Page is ready!")
    #except TimeoutException:
    #    print( "Loading took too much time!")
    
    time.sleep(5)
    form_elem=driver.find_element_by_id('mailbox:password')
    form_elem.send_keys('NewPassword172')
    form_elem.send_keys(Keys.RETURN)
    
    time.sleep(12)
    list_to_pars = get_links_to_messages()
    
    letters=[]
    for m in list_to_pars:
        item = get_info_from_message(m)
        letters.append(item)
        time.sleep(3)
    pprint(letters)
    
    
    
    client=MongoClient('localhost', 27017)
    db = client['mailru']
    messages = db.messages
    print('in mongo_dbase records number are: ', messages.count_documents({}))
    count=0
    for item in letters:
        try:
            messages.insert_one(item)
            count=count+1
        except:
            pass
    print('success write records count', count)
    print('in mongo_dbase records number are: ', messages.count_documents({}))
    


