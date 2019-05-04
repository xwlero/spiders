# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
from bili.bicook import Crack
from scrapy import signals
import time
import random
import threading
#你的账号
clist=[c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19]
cookies=[]
xianzhichi=[]
dengluchi=[]
for zhanghao in clist:
    crack = Crack(zhanghao)
    cook=crack.crack()
    cookies.append(cook)
    time.sleep(3)
def docook(zhanghao,cookie):
    global cookies
    global dengluchi
    dengluchi.append(zhanghao)
    dengluchi=list(set(dengluchi))
    try:
        cookies.remove(cookie)
    except:
        pass
    time.sleep(600)
    crack = Crack(zhanghao)
    cookk=crack.crack()
    cookies.append(cookk)
    cookies = list(set(cookies))
    try:
        dengluchi.remove(zhanghao)
    except:
        pass
def xianzhi(cookie):
    global cookies
    global xianzhichi
    try:
        cookies.remove(cookie)
    except:
        pass
    xianzhichi.append(cookie)
    xianzhichi=list(set(xianzhichi))
    time.sleep(7200)
    try:
        xianzhichi.remove(cookie)
    except:
        pass
    cookies.append(cookie)
    cookies=list(set(cookies))
class BiliSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class BiliDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.
        request.dont_filter = True
        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        cookie = random.choice(cookies)
        request.cookies = cookie[0]
        request.meta['zz'] = cookie[1]

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.
        text = response.text
        if '{\'code\': -101, \'message\': \'账号未登录\'' in text:
            zhanghao = request.meta['zz']
            cookie=request.cookies
            if zhanghao not in dengluchi:
                t = threading.Thread(target=docook, args=(zhanghao,cookie,))
                t.start()
            return request
        if '{\'code\': -509, \'message\': \'超出限制\', \'ttl\': 1}' in text:
            if request.cookies not in xianzhichi:
                t = threading.Thread(target=xianzhi, args=(request.cookies,))
                t.start()
            return request
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        else:
            return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
