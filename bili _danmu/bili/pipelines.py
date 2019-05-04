# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
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
        sql = 'insert into av_danmu(aid , up_name,up_id ,cid,message, hash ,pubdate) values(%s,%s,%s,%s,%s,%s,%s)'
        cursor.executemany(sql, item['data'])
