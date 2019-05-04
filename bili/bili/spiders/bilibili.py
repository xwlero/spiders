# -*- coding: utf-8 -*-
import scrapy
import json
import time
import math
from bili.items import BiliItem,av_info,av_comment



"""
def trans(x):
    x = x.replace('>', ',')
    x = x.split(',')
    return [x[6], x[8]]
"""
class BilibiliSpider(scrapy.Spider):
    name = 'bili'
    allowed_domains = ['bilibili.com']
    def start_requests(self):
        #up_lists=pd.read_excel('d:/spider/bili/up_id.xlsx')
        #up_lists = pd.read_excel('up_id.xlsx')
        #mids = list(up_lists['mid'])
        mids=[1920555,1890352,2424108,71608809,11346858, 6694924,325386223,2301165]

        for mid in mids:
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                # 'User-Agent': userAgent,
                 'Referer': 'http://space.bilibili.com/'+str(mid),
                # 'Origin': 'http://space.bilibili.com',
                'X-Requested-With': 'XMLHttpRequest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36'
            }
            yield scrapy.FormRequest('https://space.bilibili.com/ajax/member/GetInfo',headers=headers,formdata= {'mid': str(mid), 'csrf': 'null'},meta={'mid':mid},callback=self.parse)

    def parse(self, response):
        text=response.text
        data=json.loads(text)
        if data['status'] == True:
            data = data['data']
            item=BiliItem()
            item['mid'] = data['mid']  # 用户ID
            """
            if '=' in data['name']:
                temp = data['name'].replace('=', '')
                item['up_name'] = temp  # 用户昵称
            else:
                item['up_name'] = data['name']
            """
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
            #p_year= int(time.strftime("%Y", time_local))
            #p_month=int(time.strftime("%m", time_local))
            length=av['length']
            title=av['title']
            info=[aid,author,mid,title,length,pubdate]
            info+=tid
            yield scrapy.Request('https://api.bilibili.com/x/web-interface/view?aid='+str(aid),meta={'info':info},callback=self.parse_avinfo)#'p_year':p_year,'p_month':p_month},callback=self.parse_avinfo)
            yield scrapy.Request('https://api.bilibili.com/x/v2/reply?oid='+str(aid)+'&pn=1&type=1&sort=0&psize=20',meta={'aid':aid,'mid':mid,'pn':1},callback=self.parse_comment)
        if pn==1 and pns>1:
            for i in range(2,pns+1):
                yield scrapy.Request('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=' + str(
                    mid) + '&pagesize=30&tid=' + str(tid[1]) + '&page='+str(i)+'&order=pubdate',
                                     meta={'tid': tid, 'pn': i}, callback=self.parse_avlist)
    def parse_avinfo(self,response):
        info=response.meta['info']
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
        """
        time_local = time.localtime(time.time())
        now_year = int(time.strftime("%Y", time_local))
        now_month = int(time.strftime("%m", time_local))
        p_year=response.meta['p_year']
        p_month = response.meta['p_month']
        danmu=info[0:3]+[cid]
        if now_year - p_year == 0:
            months = list(range(p_month, now_month+1))
            for i in months:
                if i>=10:
                    date = str(now_year) + '-' + str(i)
                else:
                    date = str(now_year) + '-0' + str(i)
                yield scrapy.Request('https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + date,cookies=self.cookie,meta={'danmu':danmu},callback=self.parse_danmu)
        if now_year-p_year==1:
            if now_month<p_month:
                months=list(range(p_month,13))
                for i in months:
                    if i >= 10:
                        date=str(p_year)+'-'+str(i)
                    else:
                        date = str(p_year) + '-0' + str(i)
                    yield scrapy.Request('https://api.bilibili.com/x/v2/dm/history/index?type=1&oid='+str(cid)+'&month='+date,cookies=self.cookie,meta={'danmu':danmu},callback=self.parse_danmu)
                months = list(range(1, now_month+1))
                for i in months:
                    if i >= 10:
                        date=str(now_year)+'-'+str(i)
                    else:
                        date = str(now_year) + '-0' + str(i)
                    yield scrapy.Request('https://api.bilibili.com/x/v2/dm/history/index?type=1&oid='+str(cid)+'&month='+date,cookies=self.cookie,meta={'danmu':danmu},callback=self.parse_danmu)
        if now_year-p_year>1:
            months = list(range(now_month, 13))
            for i in months:
                if i >= 10:
                    date = str(now_year-1) + '-' + str(i)
                else:
                    date = str(now_year - 1) + '-0' + str(i)
                yield scrapy.Request(
                    'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + date,cookies=self.cookie,meta={'danmu':danmu},callback=self.parse_danmu)
            months = list(range(1, now_month + 1))
            for i in months:
                if i>=10:
                    date = str(now_year) + '-' + str(i)
                else:
                    date = str(now_year) + '-0' + str(i)
                yield scrapy.Request(
                    'https://api.bilibili.com/x/v2/dm/history/index?type=1&oid=' + str(cid) + '&month=' + date,cookies=self.cookie,meta={'danmu':danmu},callback=self.parse_danmu)
    def parse_danmu(self,response):
        text = response.text
        data = json.loads(text)
        data = data['data']
        print(data)
        info=response.meta['danmu']
        cid=info[-1]
        if data!=None:
            for i in data:
                danmu_info=info+[i]
                url='https://api.bilibili.com/x/v2/dm/history?type=1&oid='+str(cid)+'&date='+i
                yield scrapy.Request(url,cookies=self.cookie,meta={'danmu_info':danmu_info},callback=self.parse_danmu_ll)
    def parse_danmu_ll(self,response):
        text=response.text
        print(text)
        info=response.meta['danmu_info']
        te = text.replace('"', '')
        x = re.findall(self.danmu_pat, te)
        danmu = list(map(trans, x))
        item=av_danmu()
        item['aid']=info[0]
        item['up_name']=info[1]
        item['up_id'] = info[2]
        item['cid']=info[3]
        item['pubdate']=info[-1]
        item['danmu']=danmu
        yield item
    """

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





