# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy.loader.processors import MapCompose, TakeFirst
import scrapy

def get_item_var(value):
    try:
        sep = value.find(':')
        if sep <0:
            sep = value.find('—')
        if sep <0:
            val = "".join(c for c in (value) if c not in ('!','.',':',';')) 
            key = 'Text'
        else:
            key = value[:sep]
            val = value[sep+1:]
        value = {key:val}
    except:
        pass
    return value
    
def get_item_var_alt_values(value):
        try:
            value = value.replace('\n','')
            value = value.split()
            value = (" ".join(value))
        except:
            pass
        return value

class LmItem(scrapy.Item):
    # define the fields for your item here like:    
    _id                 =   scrapy.Field()
    ref                 =   scrapy.Field(output_processor=TakeFirst())
    title               =   scrapy.Field(output_processor=TakeFirst())
    price               =   scrapy.Field(output_processor=TakeFirst())
    item_var            =   scrapy.Field(input_processor =MapCompose(get_item_var))
    item_var_alt        =   scrapy.Field(input_processor =MapCompose())
    item_var_alt_values =   scrapy.Field(input_processor =MapCompose(get_item_var_alt_values))
    pics                =   scrapy.Field(input_processor =MapCompose())
    






















































