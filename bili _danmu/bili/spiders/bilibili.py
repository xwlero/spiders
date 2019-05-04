# -*- coding: utf-8 -*-
import scrapy
import json
import time
import datetime
import re
import pymysql
from bili.items import av_danmu
#import random
import pandas as pd
def trans(x):
    x = x.replace('>', ',').split(',')
    time_local = time.localtime(int(x[4]))
    pubdate=time.strftime("%Y-%m-%d  %H:%M:%S", time_local)
    return [x[8],x[6], pubdate]
class BilibiliSpider(scrapy.Spider):
    name = 'bili_danmu'
    danmu_pat = re.compile('<d p=(.*?)</d>')
    conn = pymysql.connect(host="127.0.0.1", user="xwl", passwd="hero", db="bilibili", charset="utf8mb4")
    def start_requests(self):
        #sql='select aid,up_name,up_id,pubdate,cid from bilibili.av_list having cid not in(select distinct cid from bilibili.av_danmu);'
        sql='select aid,up_name,up_id,pubdate,cid from bilibili.av_list limit 2'
        av_info=self.chaxun(sql)
        time_local = time.localtime(time.time())
        now_year = int(time.strftime("%Y", time_local))
        now_month = int(time.strftime("%m", time_local))
        for info in av_info:
            info=list(info)
            cid=info[-1]
            date=info[3]
            x=date.split('-')
            p_year=int(x[0])
            p_month=int(x[1])
            del info[3]
            if now_year - p_year == 0:
                months = list(range(p_month, now_month + 1))
                dates=[]
                for i in months:
                    if i >= 10:
                        date = str(now_year) + '-' + str(i)
                    else:
                        date = str(now_year) + '-0' + str(i)
                    dates.append(date)
            if now_year - p_year == 1:
                dates = []
                if now_month < p_month:
                    months = list(range(p_month, 13))
                    for i in months:
                        if i >= 10:
                            date = str(p_year) + '-' + str(i)
                        else:
                            date = str(p_year) + '-0' + str(i)
                        dates.append(date)
                    months = list(range(1, now_month + 1))
                    for i in months:
                        if i >= 10:
                            date = str(now_year) + '-' + str(i)
                        else:
                            date = str(now_year) + '-0' + str(i)
                        dates.append(date)
                else:
                    months = list(range(now_month, 13))
                    for i in months:
                        if i >= 10:
                            date = str(p_year) + '-' + str(i)
                        else:
                            date = str(p_year) + '-0' + str(i)
                        dates.append(date)
                    months = list(range(1, now_month + 1))
                    for i in months:
                        if i >= 10:
                            date = str(now_year) + '-' + str(i)
                        else:
                            date = str(now_year) + '-0' + str(i)
                        dates.append(date)
            if now_year - p_year > 1:
                dates = []
                months = list(range(now_month, 13))
                for i in months:
                    if i >= 10:
                        date = str(now_year - 1) + '-' + str(i)
                    else:
                        date = str(now_year - 1) + '-0' + str(i)
                    dates.append(date)
                months = list(range(1, now_month + 1))
                for i in months:
                    if i >= 10:
                        date = str(now_year) + '-' + str(i)
                    else:
                        date = str(now_year) + '-0' + str(i)
                    dates.append(date)
            pns=len(dates)-1
            yield scrapy.Request(
                'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + dates[0],
                meta={'info': info, 'dates': dates, 'p': 0,'pns':pns,'md':[]},priority=1, callback=self.parse_danmu)
            time.sleep(1)
    def chaxun(self,sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    def parse_danmu(self,response):
        pns=response.meta['pns']
        p = response.meta['p']
        dates=response.meta['dates']
        md=response.meta['md']
        text = response.text
        text = json.loads(text)
        try:
            data = text['data']
        except:
            data=None
        if data != None:
            md+=data
        info=response.meta['info']
        cid = info[-1]
        if p==pns:
            '''
            if len(md)>100:
                md=random.sample(md,100)
            '''
            if len(md)>0:
                p=0
                pns=len(md)-1
                mddate=md[p]
                url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=' + str(cid) + '&date=' + mddate
                yield scrapy.Request(url,  meta={'info': info,'data':pd.DataFrame() , 'p': 0,'pns':pns,'md':md},priority=6,callback=self.parse_danmu_ll)
            else:
                yield scrapy.Request('https://api.bilibili.com/x/v1/dm/list.so?oid=' + str(cid),
                                     meta={'info': info, 'data': pd.DataFrame(), 'p': 2, 'pns': 1, 'md': md},
                                     priority=7,callback=self.parse_danmu_ll)
        else:
            yield scrapy.Request(
                'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + dates[p+1],
                meta={'info': info, 'dates': dates, 'p': p+1, 'pns': pns, 'md': md},priority=5,callback=self.parse_danmu)

    def parse_danmu_ll(self,response):
        text=response.text
        info=response.meta['info']
        cid = info[-1]
        p = response.meta['p']
        pns= response.meta['pns']
        md = response.meta['md']
        data = response.meta['data']
        te = text.replace('"', '')
        x = re.findall(self.danmu_pat, te)
        if len(x) != 0:
            danmu = list(map(trans, x))
            danmu = pd.DataFrame(danmu)
            danmu.columns=['message', 'hash','pubdate']
            data=data.append(danmu)
            data.drop_duplicates(['message', 'hash','pubdate'], inplace=True)
        if p==pns:
            yield scrapy.Request('https://api.bilibili.com/x/v1/dm/list.so?oid='+str(cid),meta={'info': info, 'data': data, 'p': pns+100, 'pns': pns, 'md': md},priority=7,
                                 callback=self.parse_danmu_ll)
        elif p<pns:
            url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=' + str(cid) + '&date=' + md[p+1]
            yield scrapy.Request(url,
                                 meta={'info': info, 'data': data, 'p': p+1, 'pns': pns, 'md': md},priority=6,
                                 callback=self.parse_danmu_ll)
        else:
            chang = len(data)
            if chang!=0:
                info = [info] * chang
                info = pd.DataFrame(info)
                # info.columns=['aid' , 'up_name','up_id' ,'cid']
                info.index = range(len(info))
                data.index = range(len(data))
                data = pd.concat([info, data], axis=1)
                data = data.values.tolist()
                item = av_danmu()
                item['data'] = data
                yield item