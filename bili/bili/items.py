# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BiliItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    mid= scrapy.Field()
    up_name= scrapy.Field()
    sex= scrapy.Field()
    coins= scrapy.Field()
    rank= scrapy.Field()
    level= scrapy.Field()
    vip= scrapy.Field()
    fans= scrapy.Field()
    follow= scrapy.Field()
    playnum= scrapy.Field()
    movienum=scrapy.Field()
    type1=scrapy.Field()
    type1_num=scrapy.Field()
    type2 = scrapy.Field()
    type2_num = scrapy.Field()
    type3=scrapy.Field()
    type3_num=scrapy.Field()
    type4=scrapy.Field()
    type4_num=scrapy.Field()
    type5=scrapy.Field()
    type5_num=scrapy.Field()
    type6=scrapy.Field()
    type6_num=scrapy.Field()
    type7=scrapy.Field()
    type7_num=scrapy.Field()
    type8=scrapy.Field()
    type8_num=scrapy.Field()
    type9=scrapy.Field()
    type9_num=scrapy.Field()
    type10=scrapy.Field()
    type10_num=scrapy.Field()
class av_info(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    info= scrapy.Field()
class av_comment(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    comment= scrapy.Field()

