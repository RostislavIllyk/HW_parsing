from bs4 import BeautifulSoup as bs
import requests
from pprint import pprint
import numpy as np
import pandas as pd
from random import choice


def get_dic_of_occupation_from_superjob():
    main_link = 'https://www.superjob.ru/' #programmist.html'
    params = {
            }
    
    html = requests.get(main_link, params=params).text
    soup = bs(html,'lxml')
    serials_block = soup.find('ul',{'class':'_3P0J7 _9_FPy _1iZ5S _1qf3L'})
    serials_list = serials_block.findChildren(recursive=False)
      
    serials = []
    for serial in serials_list:
        item={}
        link = main_link + serial.find('a')['href'][1:]
        label = serial.find('a').getText()
        item['link'] = link
        item['label'] = label
        
        serials.append(item)
    return serials


def get_dic_of_occupation_from_hh():

    main_link = 'https://hh.ru/'
    headers = {
              'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'         
              }
        
    html = requests.get(main_link+'catalog', headers = headers)    
    html = html.text
    soup = bs(html,'lxml')
    
    serials_list = soup.find_all('div',{'class':"catalog__item"})
    serials = []
    for serial in serials_list:
        item={}
        link = main_link + serial.find('a')['href'][1:]
        label = serial.find('a').getText()
        item['link'] = link
        item['label'] = label
        serials.append(item)
        
    return serials
    


serials_sj = get_dic_of_occupation_from_superjob()
serials_hh = get_dic_of_occupation_from_hh()
serials_all = serials_sj + serials_hh

#print(len(serials_sj), len(serials_hh), len(serials_all))

for i in range (len(serials_all)):
    print(i,'\t', serials_all[i]['label'])

print()
list_of_occupation_to_search = input('Укажите через пробел номера разделов в которых будем искать. : ')
print()


list_of_occupation_to_search = list_of_occupation_to_search.split(' ')
print(list_of_occupation_to_search)
print()




list_sj=[]
list_hh=[]
try:
    for i in list_of_occupation_to_search:
        if int(i)<=len(serials_sj):
            list_sj.append(int(i))
        else:
            list_hh.append(int(i)-len(serials_sj))
except:
    print('Проверьте ввод...1')
    raise SystemExit(1)

if max(list_hh) > len(serials_hh) or min(list_hh)<0:
    print('Проверьте ввод...2')
    raise SystemExit(2)
if max(list_sj) > len(serials_sj) or min(list_sj)<0:
    print('Проверьте ввод...3')
    raise SystemExit(3)





main_link = 'https://www.superjob.ru/' #programmist.html'
params = {
          }
proxies = open('proxies.txt').read().split('\n')


data_dic=[]
for i, serial in enumerate(serials_sj):
    print(serial['label'])
    if i not in list_sj:
        continue
    
    dalshe = 'go'
    params['page']=0
    count=0
    while dalshe != 'stop':
        params['page']=params['page']+1
        proxy = {'http': 'http://' + choice(proxies)}

        html = requests.get(serial['link'],params=params, proxies=proxy, timeout=(1, 3.3)).text
        
        soup = bs(html,'lxml')
        
        all_blocks = soup.find('div',{'class':"_3Qutk"})
        all_blocks1 = all_blocks.find('div',{'style':"display:block"})
        
        serials_block = all_blocks1.find_all('div',{'class':"_3mfro CuJz5 PlM3e _2JVkc _3LJqf"})
        
        
        
        for block in serials_block:
            
            block = block.find_parent()

            
            count=count+1
            serial_data = {}
            serial_data['label'] = serial['label']
            serial_data['ref'] = None
            serial_data['name'] = None
            serial_data['salary_max'] = None
            serial_data['salary_min'] = None
            serial_data['salary_agree'] = None
            serial_data['salary_currency'] = None
            serial_data['explanation'] = None
            serial_data['who'] = None
            serial_data['where'] = None
            time_pub=''
            
            try:
                try:               
                    ref = main_link + block.find('a')['href'][1:]
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
                    explanation= block.find('span',{'class':'_3cLIl _3C76h _10Aay _2_FIo _1tH7S'}).getText()
                except:
                    explanation='Unknown'
                try:
                    who= block.find('a',{'class':'icMQ_ _205Zx f-test-link-Bilajn_ShPD _25-u7'}).getText()
                except:
                    who='Unknown'                
                try:                
                    where= block.find('span',{'class':'_3mfro f-test-text-company-item-location _9fXTd _2JVkc _2VHxz'}).getText()
                except:
                    where='Unknown'                
                try:                              
                    time_pub =  block.find('span',{'class':"_3mfro _9fXTd _2JVkc _2VHxz"}).getText()   
                    time_len=len(time_pub)
                except:
                    time_len=0              
            
            except:
                print(count, 'error')
#                break
                pass
            
            
            serial_data['ref'] = ref
            serial_data['name'] = name.replace(';', ',')
            serial_data['explanation'] = explanation.replace(';', ',')
            serial_data['who'] = who.replace(';', ',')
            where = where.replace(';', ',')
            serial_data['where'] = where[time_len+3 :]
            
            
            
            salary = salary.replace('\xa0', '')
            
            if salary == "По договорённости":
                serial_data['salary_agree'] = salary
                
            elif  salary[:2] == "от":
                serial_data['salary_min'] = int(salary[2:-4])
                serial_data['salary_currency'] = salary[-4:]
            
            elif salary[:2] == "до":
                serial_data['salary_max'] = int(salary[2:-4])
                serial_data['salary_currency'] = salary[-4:]
            
            
            elif salary.find('—') > 0:
                sep = salary.find('—')
                serial_data['salary_min'] = int(salary[: sep])
                serial_data['salary_max'] = int(salary[sep+1:-4])
                serial_data['salary_currency'] = salary[-4:]
                
            
            elif salary[0].isdigit():
                serial_data['salary_max'] = int(salary[:-4])
                serial_data['salary_min'] = int(salary[:-4])
                serial_data['salary_currency'] = salary[-4:]
            else:
                serial_data['salary_max'] = 0
                serial_data['salary_min'] = 0
                serial_data['salary_currency'] = "Unknown"
            
            
            print(serial_data['ref'])
            print(serial_data['name'])
            print(salary)
            print(serial_data['salary_max'])
            print(serial_data['salary_min'])
            print(serial_data['salary_agree'])
            print(serial_data['salary_currency'])
            print(serial_data['who'])
            print(serial_data['where'])
            print()
            print(serial_data['explanation'])
            print(count, '============================================================================')
            print()
            data_dic.append(serial_data)

            
        try:    
            dalshe=soup.find('a',{'class':'icMQ_ _1_Cht _3ze9n f-test-button-dalshe f-test-link-Dalshe'}).getText()
        except:
            dalshe='stop'
        print(dalshe)
#        break
#    
#        
#    break


database = pd.DataFrame.from_dict(data_dic)
print(database.shape)

database.to_csv('db_test_sep.csv')








#===========================================================================================
#===========================================================================================
#===========================================================================================


main_link = 'https://hh.ru/'
headers = {
          'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'         
          }

proxies = open('proxies.txt').read().split('\n')

for i, serial in enumerate(serials_hh):
    print(serial['label'])    
    if i not in list_hh:
        continue
    
    
    
    dalshe = 'go'
    params['page']=0
    count=0
    while dalshe != 'stop':
        proxy = {'http': 'http://' + choice(proxies)}
        html = requests.get(serial['link'], headers = headers, params=params, proxies=proxy, timeout=(1, 3.3)).text
        params['page']=params['page']+1
        
        soup = bs(html,'lxml')
        
        block = soup.find('div',{'class':"vacancy-serp"})
        
        serials_block = block.findChildren(recursive=False)
        

        for block in serials_block:
            count=count+1
            serial_data = {}
            serial_data['label'] = serial['label']
            serial_data['ref'] = None
            serial_data['name'] = None
            serial_data['salary_max'] = None
            serial_data['salary_min'] = None
            serial_data['salary_agree'] = None
            serial_data['salary_currency'] = None
            serial_data['explanation'] = None
            serial_data['who'] = None
            serial_data['where'] = None
            time_pub=''
            
            
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
#                break
                pass
            
            
            
            serial_data['ref'] = ref
            serial_data['name'] = name.replace(';', ',')
            serial_data['explanation'] = explanation.replace(';', ',')
            serial_data['who'] = who.replace(';', ',')
            serial_data['where'] = where.replace(';', ',')
            
            
            
            salary = salary.replace('\xa0', '')
            salary = salary.replace(' ', '')
            
            if salary == "Unknown":
                serial_data['salary_agree'] = salary
                
            elif  salary[:2] == "от":
                serial_data['salary_min'] = int(salary[2:-4])
                serial_data['salary_currency'] = salary[-4:]
            
            elif salary[:2] == "до":
                serial_data['salary_max'] = int(salary[2:-4])
                serial_data['salary_currency'] = salary[-4:]
            
            
            elif salary.find('—') > 0:
                sep = salary.find('—')
                serial_data['salary_min'] = int(salary[: sep])
                serial_data['salary_max'] = int(salary[sep+1:-4])
                serial_data['salary_currency'] = salary[-4:]
                
            elif salary.find('-') > 0:
                sep = salary.find('-')
                serial_data['salary_min'] = int(salary[: sep])
                serial_data['salary_max'] = int(salary[sep+1:-4])
                serial_data['salary_currency'] = salary[-4:]
                
            
            elif salary[0].isdigit():
                serial_data['salary_max'] = int(salary[:-4])
                serial_data['salary_min'] = int(salary[:-4])
                serial_data['salary_currency'] = salary[-4:]
            else:
                serial_data['salary_max'] = 0
                serial_data['salary_min'] = 0
                serial_data['salary_currency'] = "Unknown"
                
            
            print(serial_data['ref'])
            print(serial_data['name'])
            print(salary)
            print(serial_data['salary_max'])
            print(serial_data['salary_min'])
            print(serial_data['salary_agree'])
            print(serial_data['salary_currency'])
            print(serial_data['who'])
            print(serial_data['where'])
            print()
            print(serial_data['explanation'])
            print(count, '============================================================================')
            print()
            if serial_data['ref'] != 'Unknown':
                data_dic.append(serial_data)

            
        try:    
            dalshe=soup.find('a',{'class':"bloko-button HH-Pager-Controls-Next HH-Pager-Control"}).getText()
        except:
            dalshe='stop'
        print(dalshe)
#        break
#        
#    break


database = pd.DataFrame.from_dict(data_dic)
print(database.shape)

database.to_csv('db_hh_test_sep.csv')



#who_to_search=input('Укажите должность для поиска: ')


who_to_search="Инженер"
final_list=[]
for item in data_dic:
    status=item['name']
    result = status.find(who_to_search)
    if result >=0:
        final_list.append(item)


pprint(final_list)


final_list = pd.DataFrame.from_dict(final_list)
print(final_list.shape)
final_list.to_csv('final_list.csv')

















