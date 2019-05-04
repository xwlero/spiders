# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
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
            sql = 'insert into up_list(up_name,up_id ,sex ,coins ,rank ,level,vip,fans ,follow ,playnum ,movienum,type1  ,type1_num ,type2  ,type2_num ,type3 ,type3_num ,type4 ,type4_num ,type5 ,type5_num ,type6 ,type6_num ,type7 ,type7_num ,type8 ,type8_num ,type9 ,type9_num ,type10 ,type10_num ) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ' \
                  'ON DUPLICATE KEY UPDATE up_name=VALUES(up_name),sex=VALUES(sex),coins=VALUES(coins),rank=VALUES(rank),level=VALUES(level),vip=VALUES(vip),fans=VALUES(fans),follow=VALUES(follow),playnum=VALUES(playnum),movienum=VALUES(movienum),type1=VALUES(type1),type1_num=VALUES(type1_num),type2=VALUES(type2),type2_num=VALUES(type2_num),type3=VALUES(type3),type3_num=VALUES(type3_num)' \
                  ',type4=VALUES(type4),type4_num=VALUES(type4_num),type5=VALUES(type5),type5_num=VALUES(type5_num),type6=VALUES(type6),type6_num=VALUES(type6_num),type7=VALUES(type7),type7_num=VALUES(type7_num)' \
                  ',type8=VALUES(type8),type8_num=VALUES(type8_num),type9=VALUES(type9),type9_num=VALUES(type9_num),type10=VALUES(type10),type10_num=VALUES(type10_num);'
            args = (item['up_name'],item['mid'],item['sex'],item['coins'],item['rank'],item['level'],item['vip'],item['fans'],item['follow'],item['playnum'],item['movienum'],item['type1'],item['type1_num'],item['type2'],item['type2_num'],item['type3'],item['type3_num'],item['type4'],item['type4_num'],item['type5'],item['type5_num'],item['type6'],item['type6_num'],item['type7'],item['type7_num'],item['type8'],item['type8_num'],item['type9'],item['type9_num'],item['type10'],item['type10_num'])
            cursor.execute(sql,args)
        elif isinstance(item, av_info):
            sql = 'insert into av_list(aid ,up_name,up_id ,title,length ,pubdate ,tname1 ,tid1 ,tname2 ,tid2,cid ,coin ,danmuku ,reply ,likes,dislike ,favorite ,share ,view,max_rank ,tag1,tag2 ,tag3,tag4 ,tag5,tag6 ,tag7 ,tag8 ,tag9 ,tag10) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE tname1=VALUES(tname1),tid1=VALUES(tid1),tname2=VALUES(tname2),tid2=VALUES(tid2),coin=VALUES(coin),danmuku=VALUES(danmuku),reply=VALUES(reply),likes=VALUES(likes)' \
                  ',dislike=VALUES(dislike),favorite=VALUES(favorite),share=VALUES(share),view=VALUES(view),max_rank=VALUES(max_rank),tag1=VALUES(tag1),tag2=VALUES(tag2),tag3=VALUES(tag3),tag4=VALUES(tag4),tag5=VALUES(tag5),tag6=VALUES(tag6),tag7=VALUES(tag7),tag8=VALUES(tag8),tag9=VALUES(tag9),tag10=VALUES(tag10);'
            args=item['info']
            cursor.execute(sql, args)
        elif isinstance(item, av_comment):
            sql = 'insert into av_comment(aid , up_id ,message, user_id  ,user_name, level, vip, sex, rcount ,pubdate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            for args in item['comment']:
                cursor.execute(sql, args)