# -*- coding: utf-8 -*-
"""
Created on Tue May 19 11:31:35 2020

@author: rost_
"""

from bs4 import BeautifulSoup as bs
import requests
from pprint import pprint
import pandas as pd

from time import sleep
from random import choice, uniform


from pymongo import MongoClient
import hashlib

from multiprocessing.dummy import Pool as ThreadPool


headers = {
          'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'         
          }

class job_search:
    hh_link       = 'https://hh.ru/'
    superjob_link = 'https://www.superjob.ru/'
    params = {
            }
    headers = {
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'         
              }
    proxies = open('proxies.txt').read().split('\n')
    sj_all_pages_links_list=[]
    hh_all_pages_links_list=[]
    max_pages_from_each_section=1
    max_delay = 3
    data_dic = []


    def get_dic_of_sections_from_superjob(cls):
        html = requests.get(cls.superjob_link).text                          
        soup = bs(html,'lxml')
        sections_block = soup.find('ul',{'class':'_3P0J7 _9_FPy _1iZ5S _1qf3L'})
        sections_list = sections_block.findChildren(recursive=False)
        sections_info = []
        for section in sections_list:
            item={}
            link = cls.superjob_link + section.find('a')['href'][1:]
            label = section.find('a').getText()
            item['link'] = link
            item['label'] = label
            sections_info.append(item)
        return sections_info
    
    
    def get_dic_of_sections_from_hh(cls):
        html = requests.get(cls.hh_link+'catalog', headers = cls.headers).text    
        soup = bs(html,'lxml')
        sections_list = soup.find_all('div',{'class':"catalog__item"})
        sections_info = []
        for section in sections_list:
            item={}
            link = cls.hh_link + section.find('a')['href'][1:]
            label = section.find('a').getText()
            item['link'] = link
            item['label'] = label
            sections_info.append(item)
        return sections_info
    


    def get_one_sj_item(cls, section):
        sleep(uniform(0.21, cls.max_delay))
        pprint(section)
        link = section['link']
        for k in range(10):
            proxy = {'http': 'http://' + choice(cls.proxies)}            
            try:
                html = requests.get(link, headers = headers, proxies=proxy, timeout=(1, 3.3))
            except:
                print('Error',proxy)
                continue
            if (html.ok):
                html = html.text
                break
            else:
                raise SystemExit(1)
        
        soup = bs(html,'lxml')
        padding_block = soup.find('div',{'class':"_3zucV L1p51 undefined _2guZ- _GJem"})
        all_a = padding_block.find_all('a')
        if all_a[-1].getText() == 'Дальше':
            max_page=all_a[-2].getText()
        else:
            max_page=all_a[-1].getText()
        for i in range(int(max_page)):
            item={}
            if i >= cls.max_pages_from_each_section:
                break                
            page_link = link+'?page='+str(i+1)
            item['page_link'] = page_link
            item['label']     = section['label']
#            print("tuut=================>   ")
            cls.sj_all_pages_links_list.append(item)
        return


    def get_all_links_from_superjob(cls, sections_info):
        
#        for section in sections_info:
#            cls.get_one_sj_item(section)
        
        """ Попробуем распаралелить"""
        
        pool = ThreadPool(15)        
        pool.map(cls.get_one_sj_item, sections_info)
        pool.close()
        pool.join()        
        return cls.sj_all_pages_links_list





    def get_one_hh_item(cls, section):
        sleep(uniform(0.21, cls.max_delay))
        pprint(section)
        link = section['link']
        
        for k in range(10):
            proxy = {'http': 'http://' + choice(cls.proxies)}            
            try:
                html = requests.get(link, headers = headers, proxies=proxy, timeout=(1, 3.3))
            except:
                print('Error',proxy)
                continue
            if (html.ok):
                html = html.text
                break
            else:
                raise SystemExit(2)
        
        soup = bs(html,'lxml')
        padding_block = soup.find('div',{'data-qa':"pager-block"})                      
        all_a = padding_block.find_all('a')
        
        if all_a[-1].getText() == 'дальше':
            max_page=all_a[-2].getText()
        else:
            max_page=all_a[-1].getText()
        
        for i in range(int(max_page)):
            item={}
            if i >= cls.max_pages_from_each_section:
                break            
            page_link = link+'?page='+str(i)
            item['page_link'] = page_link
            item['label']     = section['label']
            cls.hh_all_pages_links_list.append(item)    
        return


    def get_all_links_from_hh(cls, sections_info):
        
#        for section in sections_info:
#            cls.get_one_hh_item(section)
        
        """ Попробуем распаралелить"""
        
        pool = ThreadPool(15)        
        pool.map(cls.get_one_hh_item, sections_info)
        pool.close()
        pool.join()        
        return cls.hh_all_pages_links_list
            


    def superjob_page_parser(cls, data):
        sleep(uniform(0.21, cls.max_delay))
        link = data['page_link']
        label= data['label']
        
        for k in range(10):
            proxy = {'http': 'http://' + choice(cls.proxies)}            
            try:
                html = requests.get(link, proxies=proxy, timeout=(1, 3.3))
            except:
                print('Error',proxy)
                continue
            if (html.ok):
                html = html.text
                break
            else:
                raise SystemExit(3)

        soup = bs(html,'lxml')
        all_blocks = soup.find('div',{'class':"_3Qutk"})
        all_blocks1 = all_blocks.find('div',{'style':"display:block"})
        jobs_block = all_blocks1.find_all('div',{'class':"_3mfro CuJz5 PlM3e _2JVkc _3LJqf"})
        count=0
        for block in jobs_block:
            block = block.find_parent()
            count=count+1
            jobs_data = {}
            jobs_data['label'] = label
            jobs_data['ref'] = None
            jobs_data['name'] = None
            jobs_data['salary_max'] = None
            jobs_data['salary_min'] = None
            jobs_data['salary_agree'] = None
            jobs_data['salary_currency'] = None
            jobs_data['explanation'] = None
            jobs_data['who'] = None
            jobs_data['where'] = None
            try:
                try:               
                    ref = cls.superjob_link + block.find('a')['href'][1:]
                except:
                    ref='Unknown'
                try:                               
                    name= block.find('a').getText()
                except:
                    name='Unknown'
                try:                                   
                    salary= block.find('span').getText()
                except:
                    salary='Unknown'
                block = block.find_parent()
        
                try:                                                  
                    explanation= block.find('span',{'class':'_3mfro _38T7m _9fXTd _2JVkc _2VHxz'}).getText()
                except:
                    explanation='Unknown'
                try:
                    who= block.find('span',{'class':'_3mfro _3Fsn4 f-test-text-vacancy-item-company-name _9fXTd _2JVkc _2VHxz _15msI'}).getText()
                except:
                    who='Unknown'                
                try:                
                    b= block.find('span',{'class':'_3mfro _3Fsn4 f-test-text-vacancy-item-company-name _9fXTd _2JVkc _2VHxz _15msI'})
                    b= b.findNext()
                    b= b.findNext()
                    b= b.findNext()
                    b= b.findNext()
                    b= b.findNext()
                    b= b.findNext()
                    
                    where=b.getText()
                except:
                    where='Unknown'                
            except:
                print(count, 'error')
                
            jobs_data['ref'] = ref
            jobs_data['name'] = name.replace(';', ',')
            jobs_data['explanation'] = explanation.replace(';', ',')
            jobs_data['who'] = who.replace(';', ',')
            where = where.replace(';', ',')
            jobs_data['where'] = where#[time_len+3 :]
            salary = salary.replace('\xa0', '')
            
            if salary == "По договорённости":
                jobs_data['salary_agree'] = salary
            elif  salary[:2] == "от":
                jobs_data['salary_min'] = int(salary[2:-4])
                jobs_data['salary_currency'] = salary[-4:]
            elif salary[:2] == "до":
                jobs_data['salary_max'] = int(salary[2:-4])
                jobs_data['salary_currency'] = salary[-4:]
            elif salary.find('—') > 0:
                sep = salary.find('—')
                jobs_data['salary_min'] = int(salary[: sep])
                jobs_data['salary_max'] = int(salary[sep+1:-4])
                jobs_data['salary_currency'] = salary[-4:]
            elif salary[0].isdigit():
                jobs_data['salary_max'] = int(salary[:-4])
                jobs_data['salary_min'] = int(salary[:-4])
                jobs_data['salary_currency'] = salary[-4:]
            else:
                jobs_data['salary_max'] = None
                jobs_data['salary_min'] = None
                jobs_data['salary_currency'] = None
            
            print(jobs_data['ref'])
            print(jobs_data['name'])
            print(salary)
            print(jobs_data['salary_max'])
            print(jobs_data['salary_min'])
            print(jobs_data['salary_agree'])
            print(jobs_data['salary_currency'])
            print(jobs_data['who'])
            print(jobs_data['where'])
            print()
            print(jobs_data['explanation'])
            print(count, '============================================================================')
            print()
            
            cls.data_dic.append(jobs_data)
        return cls.data_dic



    def hh_page_parser(cls, data):
        sleep(uniform(0.21, cls.max_delay))
        link = data['page_link']
        label= data['label']
        
        for k in range(10):
            proxy = {'http': 'http://' + choice(cls.proxies)}            
            try:
                html = requests.get(link, headers = headers, proxies=proxy, timeout=(1, 3.3))
            except:
                print('Error',proxy)
                continue
            if (html.ok):
                html = html.text
                break
            else:
                raise SystemExit(4)
        
        soup = bs(html,'lxml')
        block = soup.find('div',{'class':"vacancy-serp"})
        jobs_block = block.findChildren(recursive=False)
        count=0        
        for block in jobs_block:
            count=count+1
            jobs_data = {}
            jobs_data['label'] = label
            jobs_data['ref'] = None
            jobs_data['name'] = None
            jobs_data['salary_max'] = None
            jobs_data['salary_min'] = None
            jobs_data['salary_agree'] = None
            jobs_data['salary_currency'] = None
            jobs_data['explanation'] = None
            jobs_data['who'] = None
            jobs_data['where'] = None
            try:
                try:               
                    ref = block.find('a',{'class':"bloko-link HH-LinkModifier"})['href']
                except:
                    ref='Unknown'
                try:                               
                    name= block.find('a',{'class':"bloko-link HH-LinkModifier"}).getText()
                except:
                    name='Unknown'
                try:                                   
                    salary= block.find('span',{'class':'bloko-section-header-3 bloko-section-header-3_lite', 'data-qa':"vacancy-serp__vacancy-compensation"}).getText()
                except:
                    salary='Unknown'
                try:                                                  
                    explanation= block.find('div',{'class':"g-user-content"}).getText()
                except:
                    explanation='Unknown'
                try:
                    who= block.find('a',{'class':'bloko-link bloko-link_secondary','data-qa':"vacancy-serp__vacancy-employer"}).getText()
                except:
                    who='Unknown'                
                try:                
                    where= block.find('span',{'class':"vacancy-serp-item__meta-info"}).getText()
                except:
                    where='Unknown'                
            except:
                print(count, 'error')
                
            jobs_data['ref'] = ref
            jobs_data['name'] = name.replace(';', ',')
            jobs_data['explanation'] = explanation.replace(';', ',')
            jobs_data['who'] = who.replace(';', ',')
            jobs_data['where'] = where.replace(';', ',')
            salary = salary.replace('\xa0', '')
            salary = salary.replace(' ', '')
            
            if salary == "Unknown":
                jobs_data['salary_agree'] = salary
            elif  salary[:2] == "от":
                if salary[-4]=='р':
                    jobs_data['salary_currency'] = salary[-4:]
                    jobs_data['salary_min'] = int(salary[2:-4])   
                else:
                    jobs_data['salary_currency'] = salary[-3:]
                    jobs_data['salary_min'] = int(salary[2:-3])   
            elif salary[:2] == "до":
                if salary[-4]=='р':                
                    jobs_data['salary_currency'] = salary[-4:]
                    jobs_data['salary_max'] = int(salary[2:-4])   
                else:
                    jobs_data['salary_currency'] = salary[-3:]
                    jobs_data['salary_max'] = int(salary[2:-3])   
            elif salary.find('—') > 0:
                if salary[-4]=='р':                                
                    jobs_data['salary_currency'] = salary[-4:]              
                    sep = salary.find('—')
                    jobs_data['salary_min'] = int(salary[: sep])
                    jobs_data['salary_max'] = int(salary[sep+1:-4])
                else:
                    jobs_data['salary_currency'] = salary[-3:]              
                    sep = salary.find('—')
                    jobs_data['salary_min'] = int(salary[: sep])
                    jobs_data['salary_max'] = int(salary[sep+1:-3])
            elif salary.find('-') > 0:
                if salary[-4]=='р':                                
                    jobs_data['salary_currency'] = salary[-4:]              
                    sep = salary.find('-')
                    jobs_data['salary_min'] = int(salary[: sep])
                    jobs_data['salary_max'] = int(salary[sep+1:-4])
                else:
                    jobs_data['salary_currency'] = salary[-3:]              
                    sep = salary.find('-')
                    jobs_data['salary_min'] = int(salary[: sep])
                    jobs_data['salary_max'] = int(salary[sep+1:-3])
            elif salary[0].isdigit():
                if salary[-4]=='р':                                
                    jobs_data['salary_max'] = int(salary[:-4])
                    jobs_data['salary_min'] = int(salary[:-4])
                    jobs_data['salary_currency'] = salary[-4:]
                else:
                    jobs_data['salary_max'] = int(salary[:-3])
                    jobs_data['salary_min'] = int(salary[:-3])
                    jobs_data['salary_currency'] = salary[-3:]
            else:
                jobs_data['salary_max'] = 0
                jobs_data['salary_min'] = 0
                jobs_data['salary_currency'] = "Unknown"
            
            print(jobs_data['ref'])
            print(jobs_data['name'])
            print(salary)
            print(jobs_data['salary_max'])
            print(jobs_data['salary_min'])
            print(jobs_data['salary_agree'])
            print(jobs_data['salary_currency'])
            print(jobs_data['who'])
            print(jobs_data['where'])
            print()
            print(jobs_data['explanation'])
            print(count, '============================================================================')
            print()
            if jobs_data['ref'] != 'Unknown':
                cls.data_dic.append(jobs_data)
        
        return cls.data_dic









if __name__ == '__main__':
    job_request=job_search()
    sections_sj = job_request.get_dic_of_sections_from_superjob()
    sections_hh = job_request.get_dic_of_sections_from_hh()
    pages_link_sj = job_request.get_all_links_from_superjob(sections_sj)
    pages_link_hh = job_request.get_all_links_from_hh(sections_hh)
    
    
    
#    for data in pages_link_sj:
#        job_request.superjob_page_parser(data)
#    for data in pages_link_hh:
#        job_request.hh_page_parser(data)
    
    """ Попробуем распаралелить"""
    
    pool = ThreadPool(15)        
    pool.map(job_request.superjob_page_parser, pages_link_sj)
    pool.close()
    pool.join()        
    
    pool = ThreadPool(15)        
    pool.map(job_request.hh_page_parser, pages_link_hh)
    pool.close()
    pool.join()        
    
    
    
    print()
    print()
#    pprint(job_request.data_dic[:5])
    print('Total records are: ',len(job_request.data_dic))
    print()
    print()

#===================================================================================================================
    """
    Написать функцию, которая будет добавлять в вашу базу данных только новые вакансии с сайта
    
    Решим данную задачу путем задания каждой записи уникального '_id' привязанного к 
    'ref' - уникальному полю записи.
    
    Новую запись с уже имеющимся '_id' mongodb не добавит.
    """

    for job_item in job_request.data_dic:
        jod_item_str=str(job_item['ref'])
        jod_item_bin = jod_item_str.encode('utf-8')
        jod_item_hash = hashlib.md5(jod_item_bin)
        job_item['_id'] = jod_item_hash.hexdigest()

    database = pd.DataFrame.from_dict(job_request.data_dic)
    print(database.shape)    
    database.to_csv('db_3d.csv')



    client=MongoClient('localhost', 27017)
    db = client['job_search']
    job = db.job
    print('in mongo_dbase records number are: ', job.count_documents({}))
    
    count=0
    for item in job_request.data_dic:
        try:
            job.insert_one(item)
            count=count+1
        except:
            pass
    print('success write records count', count)

    count=0
    a=job.find({})
    for i in a:
        count=count+1
    print('Total count read', count)
    print()
    print()
    
    
#===================================================================================================================

    """
    Добавим новую, тестовую запись, в исходные данные и попытаемся снова все 
    данные записать в базу.
    """


    test_record = {'_id': 'test',
         'label': 'test',
         'ref': 'test',
         'name': 'test',
         'salary_max': None,
         'salary_min': None,
         'salary_agree': None,
         'salary_currency': None,
         'explanation': 'test',
         'who': 'test',
         'where': 'test'}
    job_request.data_dic.append(test_record)


    count=0
    for item in job_request.data_dic:
        try:
            job.insert_one(item)
            count=count+1
        except:
            pass
    print('success write records count', count)
    print('in mongo_dbase records number are: ', job.count_documents({}))
    print()
    print()
#===================================================================================================================
    """
    Написать функцию, которая производит поиск и выводит на экран вакансии с 
        заработной платой больше введенной суммы.
    """

    def get_salary_more_then(fild, level):

        job_list = job.find({fild:{'$gt':level}})
        job_list = list(job_list)
        
        return job_list


    fild = 'salary_max'
    level = 30000
    job_list = get_salary_more_then(fild, level)
    pprint(job_list[:5])
    print()
    print('Total records amount are: ' , len(job_list))









