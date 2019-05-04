# -*- coding: utf-8 -*-
import scrapy
import json
import time
import datetime
import re
import pymysql
from bili.items import av_danmu
import random
import pandas as pd

class BilibiliSpider(scrapy.Spider):
    name = 'bili_danmu'
    danmu_pat = re.compile('<d p=(.*?)</d>')
    conn = pymysql.connect(host="127.0.0.1", user="xwl", passwd="hero", db="bilibili", charset="utf8mb4")
    def start_requests(self):
        sql='select a.aid,a.up_name,a.up_id,a.pubdate,a.cid from av_danmu  a JOIN(select aid,max(pubdate) pubdate from av_danmu group by aid) b on a.aid=b.aid and a.pubdate=b.pubdate group by aid,up_name,up_id,cid,pubdate;'
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
            paquriqi = datetime.datetime.strptime(date, "%Y-%m-%d")+datetime.timedelta(days=1)
            seconds = time.mktime(paquriqi.timetuple())
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
                meta={'info': info, 'dates': dates,'seconds':seconds,'paquriqi':paquriqi, 'p': 0,'pns':pns,'md':[]},priority=1, callback=self.parse_danmu)
            time.sleep(1)
    def chaxun(self,sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    def parse_danmu(self,response):
        paquriqi=response.meta['paquriqi']
        pns=response.meta['pns']
        p = response.meta['p']
        dates=response.meta['dates']
        seconds=response.meta['seconds']
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
            if len(md)>100:
                md=random.sample(md,100)
            if len(md)>0:
                for i in md:
                    if datetime.datetime.strptime(i, "%Y-%m-%d")<paquriqi:
                        md.remove(i)
                p=0
                pns=len(md)-1
                mddate=md[p]
                url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=' + str(cid) + '&date=' + mddate
                yield scrapy.Request(url,  meta={'info': info,'data':pd.DataFrame() ,'seconds':seconds, 'p': 0,'pns':pns,'md':md},priority=6,callback=self.parse_danmu_ll)
            else:
                yield scrapy.Request('https://api.bilibili.com/x/v1/dm/list.so?oid=' + str(cid),
                                     meta={'info': info, 'seconds':seconds,'data': pd.DataFrame(), 'p': 2, 'pns': 1, 'md': md},
                                     priority=7,callback=self.parse_danmu_ll)
        else:
            yield scrapy.Request(
                'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + dates[p+1],
                meta={'info': info, 'dates': dates,'seconds':seconds,'paquriqi':paquriqi, 'p': p+1, 'pns': pns, 'md': md},priority=5,callback=self.parse_danmu)

    def parse_danmu_ll(self,response):
        text=response.text
        info=response.meta['info']
        cid = info[-1]
        seconds = response.meta['seconds']
        p = response.meta['p']
        pns= response.meta['pns']
        md = response.meta['md']
        data = response.meta['data']
        te = text.replace('"', '')
        x = re.findall(self.danmu_pat, te)
        if len(x) != 0:
            danmu = list(map(trans, x,[seconds]*len(x)))
            danmu=list(filter(None,danmu))
            danmu = pd.DataFrame(danmu)
            if len(danmu)!=0:
                danmu.columns=['message', 'hash','pubdate']
                data=data.append(danmu)
                data.drop_duplicates(['message', 'hash','pubdate'], inplace=True)
        if p==pns:
            yield scrapy.Request('https://api.bilibili.com/x/v1/dm/list.so?oid='+str(cid),meta={'info': info, 'seconds':seconds,'data': data, 'p': pns+100, 'pns': pns, 'md': md},priority=7,
                                 callback=self.parse_danmu_ll)
        elif p<pns:
            url = 'https://api.bilibili.com/x/v2/dm/history?type=1&oid=' + str(cid) + '&date=' + md[p+1]
            yield scrapy.Request(url,meta={'info': info, 'seconds':seconds,'data': data, 'p': p+1, 'pns': pns, 'md': md},priority=6,callback=self.parse_danmu_ll)
        else:
            chang = len(data)
            if chang!=0:
                info = [info] * chang
                info = pd.DataFrame(info)
                info.index = range(len(info))
                data.index = range(len(data))
                data = pd.concat([info, data], axis=1)
                data = data.values.tolist()
                item = av_danmu()
                item['data'] = data
                yield item
def trans(x,seconds):
    x = x.replace('>', ',').split(',')
    pub=int(x[4])
    if pub>seconds:
        time_local = time.localtime(pub)
        pubdate=time.strftime("%Y-%m-%d  %H:%M:%S", time_local)
        return [x[8],x[6], pubdate]