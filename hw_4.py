# -*- coding: utf-8 -*-
"""
Created on Fri May 22 20:05:20 2020

@author: rost_
"""

from lxml import html
import requests
from pprint import pprint
from datetime import datetime, timedelta
from random import choice, uniform
from time import sleep
import hashlib


from sqlalchemy import create_engine
from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

#from multiprocessing.dummy import Pool as ThreadPool




engine = create_engine('sqlite:///news.db',echo=True)
Base = declarative_base()



class Get_news:
    
    headers ={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)\
              AppleWebKit/537.36 (KHTML, like Gecko) \
              Chrome/83.0.4103.61 Safari/537.36",
              "Connection" : "close"}
    
    all_news      =[]
    lenta_ru_news =[]
    yandex_ru_news=[]
    mail_ru_news  =[]
    today     = (datetime.now()).strftime('%Y-%m-%d')
    yerstaday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    
    proxies = open('proxies.txt').read().split('\n')
    max_delay=1.5

    def get_page(cls, link):
        
        response = requests.get(link, headers=cls.headers)
        if (response.ok):
            return response
        
        for k in range(10):
            proxy = {'http': 'http://' + choice(cls.proxies)}       
            try:
                response = requests.get(link, headers = cls.headers, proxies=proxy, timeout=(1, 1))
            except:
                print('Error:   ',proxy)
                sleep(uniform(0.21, cls.max_delay))
                continue
            if (response.ok):
                break
            else:
                raise SystemExit(1)
        return response


    def get_hash(cls, text):
        text_str=str(text)
        text_bin = text_str.encode('utf-8')
        text_hash = hashlib.md5(text_bin)
        text_hash = text_hash.hexdigest()
        return text_hash



    def get_lenta_news(cls):
        
        main_link = "https://lenta.ru/"
        responce = cls.get_page(main_link)
        dom = html.fromstring(responce.text)        
        result = dom.xpath('//section[@class="row b-top7-for-main js-top-seven"]//div[contains(@class,"item")]//a')
        
        for res in result:
            news={}
            who ='lenta.ru'
            try:
                time = res.xpath('. /time/text()')[0]
                text = res.xpath('. /text()')[0].replace('\xa0',' ')
                ref  = main_link + res.xpath('. /@href')[0][1:]
            except:
                continue
            news['date'] = cls.today    
            news['time'] = time    
            news['ref']  = ref
            news['text'] = text
            news['who']  = who
            news['link_hash'] = cls.get_hash(ref)
            cls.lenta_ru_news.append(news)
        cls.all_news = cls.lenta_ru_news + cls.yandex_ru_news + cls.mail_ru_news
        
        return cls.lenta_ru_news
    
  
    
    
    def get_yandex_news(cls):
    
        main_link = "https://yandex.ru/"
        work_link = main_link+"news/"
        
        responce = cls.get_page(work_link)
        dom = html.fromstring(responce.text)
        result = dom.xpath('//div[@class="story story_view_main"] | //td[@class="stories-set__item"]')
        
        for i, res in enumerate(result):
            news={}
            try:
                who_time = res.xpath('. //div[@class="story__date"]/text()')[0]
                text = res.xpath('. //a/text()')
                ref  = res.xpath('. //a/@href')
            except:
                print('oops')
                continue
            
            if i<5:
                ref  = main_link + ref[1][1:]
                text = text[1]
                
            else:
                ref = main_link + ref[0][1:]
                text = text[0]
                
            news['date'] = cls.today
            who = who_time[:-5]
            time = who_time[-5:]
            if who[-8:] == "вчера\xa0в\xa0":
                who = who[:-8]
                news['date'] = cls.yerstaday
                
            news['time'] = time    
            news['ref']  = ref
            news['text'] = text
            news['who']  = who
            news['link_hash'] = cls.get_hash(ref)
            
            cls.yandex_ru_news.append(news)
            
        cls.all_news = cls.lenta_ru_news + cls.yandex_ru_news + cls.mail_ru_news

        return cls.yandex_ru_news
    
 


    def get_mailru_news(cls):
        
        work_link = "https://news.mail.ru/"
        responce = cls.get_page(work_link)
        
        dom = html.fromstring(responce.text)
        pages = dom.xpath('//div[@class="cols__inner"]//li//a | //a[@class="newsitem__title link-holder"]')
        
        def get_mailru_page(ref):

            news={}
            responce = cls.get_page(ref)
            
            dom = html.fromstring(responce.text)
            blocks = dom.xpath('//div[@class="article js-article js-module"]')
            time_who = blocks[0].xpath('. //span[@class="note"]//text()')
            date_time=blocks[0].xpath('. //span[@class="note"]//@datetime')[0]    
            
            news['date'] = date_time[:10]
            news['time'] = date_time[11:16]
            news['ref'] = ref
            news['text'] = blocks[0].xpath('. //h1/text()')[0]
            news['who'] =time_who[2]
            news['link_hash'] = cls.get_hash(ref)
            
            cls.mail_ru_news.append(news)
        
            return


          
        mail_news_pages=[]
        for i, res in enumerate(pages):
            ref=work_link+res.xpath('. /@href')[0][1:]
            mail_news_pages.append(ref)
        
        for i, ref in enumerate(mail_news_pages):
            get_mailru_page(ref)        


#        pool = ThreadPool(15)        
#        pool.map(get_mailru_page, mail_news_pages)
#        pool.close()
#        pool.join()        
#        cls.all_news = cls.lenta_ru_news + cls.yandex_ru_news + cls.mail_ru_news
            
        return cls.mail_ru_news
    
    
    
class News_item(Base):
    __tablename__ = 'news'
    _id      = Column(String, primary_key=True,
                      unique=True ,sqlite_on_conflict_unique='IGNORE')
    date    = Column(String(255))
    time    = Column(String(255))    
    ref     = Column(String(255))
    text    = Column(String(255))
    who     = Column(String(255))  

    def __init__(self, _id, date, time, ref, text, who):
        self._id     = _id
        self.date    = date
        self.time    = time
        self.ref     = ref
        self.text    = text
        self.who     = who
    
    
    
    




if __name__ == '__main__':

    news=Get_news()    
    
    lenta_news=news.get_lenta_news()
    pprint(lenta_news)
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    yandex_news=news.get_yandex_news()
    pprint(yandex_news)
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    mailru_news=news.get_mailru_news()
    pprint(mailru_news)
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    
    print(len(news.all_news))    


    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    
    session = Session()
    for news_content in news.all_news:
        news_record=News_item(news_content['link_hash'], news_content['date'], 
                              news_content['time'], news_content['ref'], 
                              news_content['text'], news_content['who'])
        try:
            session.add(news_record)
            session.commit()
        except:
            pass

    session.close()
    
    print()
    print()
    print('Read from dbase: ')
    
    session = Session()
    for i, instance in enumerate (session.query(News_item)):
        print(instance.date, instance.time)
        print(instance.text)
        print(i+1, "==================================================================")
        print()
    session.close()






session = Session()
num_rows_deleted = session.query(News_item).delete()
session.commit()



    