# -*- coding: utf-8 -*-
import scrapy
import json
import time
import math
import re
from bili.items import BiliItem,av_info,av_comment
import pymysql

def trantuple(x):
    return x[0]
class BilibiliSpider(scrapy.Spider):
    name = 'bili'
    allowed_domains = ['bilibili.com']
    conn = pymysql.connect(host="", user="", passwd="", db="", charset="")
    def start_requests(self):
        sql = 'select up_id from bilibili.up_list limit;'
        mids=self.chaxun(sql)
        mids=list(map(trantuple,mids))
        for mid in mids:
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                 'Referer': 'http://space.bilibili.com/'+str(mid),
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
            }
            yield scrapy.FormRequest('https://space.bilibili.com/ajax/member/GetInfo',headers=headers,formdata= {'mid': str(mid), 'csrf': 'null'},meta={'mid':mid},callback=self.parse)
    def chaxun(self,sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
    def parse(self, response):
        text=response.text
        data=json.loads(text)
        if data['status'] == True:
            data = data['data']
            item=BiliItem()
            item['mid'] = data['mid']  # 用户ID
            item['up_name'] = data['name']
            item['sex'] = data['sex'] if data['sex'] != '' else 'None'  # 用户性别
            item['coins'] = data['coins']  # 用户B币数
            item['rank'] = data['rank']
            item['level'] = data['level_info']['current_level']  # 用户当前等级
            item['vip'] = data['vip']['vipStatus']
        yield scrapy.Request('https://api.bilibili.com/x/relation/stat?vmid='+str(response.meta['mid']),meta={'item':item},callback=self.parse_up_relation)
    def parse_up_relation(self,response):
        text = response.text
        data = json.loads(text)
        data=data['data']
        item=response.meta['item']
        item['fans']=data['follower'] #粉丝
        item['follow']=data['following']#关注
        yield scrapy.Request('https://api.bilibili.com/x/space/upstat?mid='+str(item['mid']),meta={'item':item},callback=self.parse_upinfo)
    def parse_upinfo(self,response):
        text = response.text
        data = json.loads(text)
        item=response.meta['item']
        item['playnum']=data['data']['archive']['view'] #播放量
        yield scrapy.Request('https://space.bilibili.com/ajax/member/getSubmitVideos?mid='+str(item['mid'])+'&pagesize=30&tid=0&page=1&order=pubdate',meta={'item':item},callback=self.parse_av)
    def parse_av(self,response):
        text = response.text
        data = json.loads(text)
        data = data['data']
        item = response.meta['item']
        item['movienum']=data['count']
        types = data['tlist']
        ty_list=[]
        tids=[]
        for i in types:
            ty=types[i]
            x1=[ty['name'],ty['count']]
            tid=[ty['name'],ty['tid']]
            tids.append(tid)
            ty_list+=x1
        if len(ty_list)<20:
            x2=['']*(20-len(ty_list))
            ty_list +=x2
        for i in range(10):
            item['type'+str(i+1)]=ty_list[2*i]
            item['type'+str(i+1)+'_num']=ty_list[2*i+1]
        yield item
        for tid in tids:
            yield scrapy.Request('https://space.bilibili.com/ajax/member/getSubmitVideos?mid='+str(item['mid'])+'&pagesize=30&tid='+str(tid[1])+'&page=1&order=pubdate',meta={'tid':tid,'pn':1},callback=self.parse_avlist)
    def parse_avlist(self,response):
        text = response.text
        data = json.loads(text)
        data = data['data']
        pn = response.meta['pn']
        pns=data['pages']
        vlist=data['vlist']
        tid=response.meta['tid']
        for av in vlist:
            aid=av['aid']
            author=av['author']
            mid=av['mid']
            time_local = time.localtime(av['created'])
            pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            length=av['length']
            title=av['title']
            info=[aid,author,mid,title,length,pubdate]
            info+=tid
            yield scrapy.Request('https://api.bilibili.com/x/web-interface/view?aid='+str(aid),meta={'info':info},callback=self.parse_avinfo)
        if pn==1 and pns>1:
            for i in range(2,pns+1):
                yield scrapy.Request('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=' + str(mid) + '&pagesize=30&tid=' + str(tid[1]) + '&page='+str(i)+'&order=pubdate',
                                     meta={'tid': tid, 'pn': i}, callback=self.parse_avlist)
    def parse_avinfo(self,response):
        info=response.meta['info']
        mid=info[2]
        text = response.text
        data = json.loads(text)
        data = data['data']
        aid=data['aid']
        cid=data['cid']
        tname=data['tname']
        tid=data['tid']
        stat=data['stat']
        coin=stat['coin']
        danmuku=stat['danmaku']
        reply=stat['reply']
        like=stat['like']
        dislike=stat['dislike']
        favorite=stat['favorite']
        share=stat['share']
        view=stat['view']
        max_rank=stat['his_rank']
        new=[tname,tid,cid,coin,danmuku,reply,like,dislike,favorite,share,view,max_rank]
        info+=new
        yield scrapy.Request('https://api.bilibili.com/x/tag/archive/tags?aid='+str(aid),meta={'info':info},callback=self.parse_avtag)
        sql = 'select reply from bilibili.av_list where aid=%d' % aid
        try:
            reply_old = int(self.chaxun(sql)[0][0])
            dif=reply-reply_old
            if dif>0:
                pns = math.ceil(dif / 20)
                last_pn=dif%20
                if pns==1:
                    yield scrapy.Request( 'https://api.bilibili.com/x/v2/reply?oid=' + str(aid) + '&pn=1&type=1&sort=0&psize=20',
                        meta={'aid': aid, 'mid': mid, 'last_pn':last_pn}, callback=self.parse_comment_update)
                else:
                    for pn in range(1,pns):
                        yield scrapy.Request(
                            'https://api.bilibili.com/x/v2/reply?oid=' + str(aid) + '&pn='+str(pn)+'&type=1&sort=0&psize=20',
                            meta={'aid': aid, 'mid': mid,  'last_pn': 20},
                            callback=self.parse_comment_update)
                    yield scrapy.Request(
                        'https://api.bilibili.com/x/v2/reply?oid=' + str(aid) + '&pn=' + str(
                            pn) + '&type=1&sort=0&psize=20',
                        meta={'aid': aid, 'mid': mid,  'last_pn': last_pn},
                        callback=self.parse_comment_update)
        except:
            yield scrapy.Request('https://api.bilibili.com/x/v2/reply?oid=' + str(aid) + '&pn=1&type=1&sort=0&psize=20',
                             meta={'aid': aid, 'mid': mid, 'pn': 1}, callback=self.parse_comment)
    def parse_avtag(self,response):
        info = response.meta['info']
        text = response.text
        data = json.loads(text)
        data = data['data']
        tags = []
        for i in data:
            tags.append(i['tag_name'])
        len_tag=len(tags)
        if len_tag==10:
            pass
        elif len_tag<10:
            tags+=['']*(10-len_tag)
        else:
            tags=tags[0:10]
        info+=tags
        item=av_info()
        item['info']=info
        yield item
    def parse_comment(self,response):
        aid = response.meta['aid']
        mid=response.meta['mid']
        text = response.text
        data = json.loads(text)
        data=data['data']
        pn=response.meta['pn']
        if pn==1:
            pns=data['page']['count']
            pns=math.ceil(pns/20)
            for i in range(2,pns+1):
                yield scrapy.Request('https://api.bilibili.com/x/v2/reply?oid='+str(aid)+'&pn='+str(i)+'&type=1&sort=0&psize=20',meta={'aid':aid,'mid':mid,'pn':i},callback=self.parse_comment)
        coms=data['replies']
        comments=[]
        for com in coms:
            content = com['content']
            message = content['message']
            member = com['member']
            time_local = time.localtime(com['ctime'])
            pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            user_id = member['mid']
            user_name = member['uname']
            level = member['level_info']['current_level']
            sex = member['sex']
            vip = member['vip']['vipStatus']
            rcount=com['rcount']
            comment=[aid,mid,message,user_id,user_name,level,vip,sex,rcount,pubdate]
            comments.append(comment)
            if rcount>0 and rcount<=3:
                com_replies = com['replies']
                for comm in com_replies:
                    content = comm['content']
                    time_local = time.localtime(comm['ctime'])
                    pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                    message = content['message']
                    member = comm['member']
                    user_id = member['mid']
                    user_name = member['uname']
                    level = member['level_info']['current_level']
                    sex = member['sex']
                    vip = member['vip']['vipStatus']
                    rcount = comm['rcount']
                    comment = [aid, mid, message, user_id, user_name, level, vip, sex, rcount,pubdate]
                    comments.append(comment)
            if rcount>3:
                pns=math.ceil(rcount/10)
                rpid=com['rpid']
                for pn in range(1,pns+1):
                    yield scrapy.Request('https://api.bilibili.com/x/v2/reply/reply?pn='+str(pn)+'&type=1&oid='+str(aid)+'&ps=10&root='+str(rpid),meta={'aid':aid,'mid':mid,'pn':2},callback=self.parse_comment)
        item=av_comment()
        item['comment']=comments
        yield item
    def parse_comment_update(self,response):
        aid = response.meta['aid']
        mid=response.meta['mid']
        last_pn=response.meta['last_pn']
        text = response.text
        data = json.loads(text)
        data=data['data']
        if last_pn==20:
            coms = data['replies']
        else:
            coms=data['replies'][:last_pn]
        comments=[]
        for com in coms:
            content = com['content']
            message = content['message']
            member = com['member']
            time_local = time.localtime(com['ctime'])
            pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            user_id = member['mid']
            user_name = member['uname']
            level = member['level_info']['current_level']
            sex = member['sex']
            vip = member['vip']['vipStatus']
            rcount=com['rcount']
            comment=[aid,mid,message,user_id,user_name,level,vip,sex,rcount,pubdate]
            comments.append(comment)
            if rcount>0 and rcount<=3:
                com_replies = com['replies']
                for comm in com_replies:
                    content = comm['content']
                    time_local = time.localtime(comm['ctime'])
                    pubdate = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                    message = content['message']
                    member = comm['member']
                    user_id = member['mid']
                    user_name = member['uname']
                    level = member['level_info']['current_level']
                    sex = member['sex']
                    vip = member['vip']['vipStatus']
                    rcount = comm['rcount']
                    comment = [aid, mid, message, user_id, user_name, level, vip, sex, rcount,pubdate]
                    comments.append(comment)
            if rcount>3:
                pns=math.ceil(rcount/10)
                rpid=com['rpid']
                for pn in range(1,pns+1):
                    yield scrapy.Request('https://api.bilibili.com/x/v2/reply/reply?pn='+str(pn)+'&type=1&oid='+str(aid)+'&ps=10&root='+str(rpid),meta={'aid':aid,'mid':mid,'last_pn':20},callback=self.parse_comment_update)
        item=av_comment()
        item['comment']=comments
        yield item




