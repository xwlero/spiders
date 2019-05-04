# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
import numpy as np
from bili.items import BiliItem,av_info,av_comment
from twisted.enterprise import adbapi
class BiliPipeline(object):
    def __init__(self, ):
        dbparms = dict(
            host='',
            db='',
            user='',
            passwd='',
            charset='',
            cursorclass=pymysql.cursors.DictCursor, # 指定 curosr 类型
            use_unicode=True,
        )
        # 指定擦做数据库的模块名和数据库参数参数
        self.dbpool = adbapi.ConnectionPool("pymysql", **dbparms)

    # 使用twisted将mysql插入变成异步执行
    def process_item(self, item, spider):
        # 指定操作方法和操作的数据
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 指定异常处理方法
        query.addErrback(self.handle_error, item, spider) #处理异常

    def handle_error(self, failure, item, spider):
        #处理异步插入的异常
        print(failure)
    def do_insert(self, cursor, item):
        if isinstance(item, BiliItem):
            sql = 'insert into up_list(up_name,up_id ,sex ,coins ,rank ,level,vip,fans ,follow ,playnum ,movienum,type1  ,type1_num ,type2  ,type2_num ,type3 ,type3_num ,type4 ,type4_num ,type5 ,type5_num ,type6 ,type6_num ,type7 ,type7_num ,type8 ,type8_num ,type9 ,type9_num ,type10 ,type10_num ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
            args = (item['up_name'],item['mid'],item['sex'],item['coins'],item['rank'],item['level'],item['vip'],item['fans'],item['follow'],item['playnum'],item['movienum'],item['type1'],item['type1_num'],item['type2'],item['type2_num'],item['type3'],item['type3_num'],item['type4'],item['type4_num'],item['type5'],item['type5_num'],item['type6'],item['type6_num'],item['type7'],item['type7_num'],item['type8'],item['type8_num'],item['type9'],item['type9_num'],item['type10'],item['type10_num'])
            cursor.execute(sql,args)
        elif isinstance(item, av_info):
            sql = 'insert into av_list(aid ,up_name,up_id ,title,length ,pubdate ,tname1 ,tid1 ,tname2 ,tid2,cid ,coin ,danmuku ,reply ,likes,dislike ,favorite ,share ,view,max_rank ,tag1,tag2 ,tag3,tag4 ,tag5,tag6 ,tag7 ,tag8 ,tag9 ,tag10) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            args=item['info']
            cursor.execute(sql, args)
        elif isinstance(item, av_comment):
            sql = 'insert into av_comment(aid , up_id ,message, user_id  ,user_name, level, vip, sex, rcount ,pubdate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            for args in item['comment']:
                cursor.execute(sql, args)
        """
        elif isinstance(item, av_danmu):
            sql1 = 'insert into av_danmu(aid , up_name,up_id ,cid,message, user_id  ,pubdate) values(%s,%s,%s,%s,%s,%s,%s)'
            sql2='insert into av_danmu(aid , up_name,up_id ,cid,message, hash ,pubdate) values(%s,%s,%s,%s,%s,%s,%s)'
            for content in item['danmu']:
                d1=content[0]
                d2=content[1]
                s1='select user_id from bilibili.hash_table where hash=%s' %d1
                cursor.execute(s1)
                user_id=cursor.fetchone()[0]
                args=(item['aid'],item['up_name'],item['up_id'],item['cid'],d2,user_id,item['pubdate'])
                cursor.execute(sql1, args)
            for content in item['danmu']:
                d1=content[0]
                d2=content[1]
                args = (item['aid'], item['up_name'], item['up_id'], item['cid'], d2, d1, item['pubdate'])
                cursor.execute(sql2, args)
        """