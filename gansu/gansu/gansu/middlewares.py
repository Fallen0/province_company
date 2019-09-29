# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import base64
import math
import time
import datetime
import requests
from scrapy import signals


class ExceptionSpiderMiddleware:
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
        with open('error.txt', 'a') as f:
            f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.status}--{response.url}--{exception.__repr__()}\n")
            f.flush()
            print()

'''
def get_proxy():
    res = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=966404044351881&num=1&pt=1&f_et=1&format=json&sep=1')
    if res.json()['code'] != 0:
        return get_proxy()
    proxy = res.json()['data']['proxy_list'][0]
    proxy, expire_date = proxy.split(',')[0], proxy.split(',')[1]
    return 'http://zhao_tai_yu:7av2i9t5@' + proxy, expire_date


class ProxyMiddleware(object):
    expire_date = 0
    proxy = None

    def process_request(self, request, spider):
        if self.expire_date < math.floor(time.time()) + 15:
            proxy, expire_date = get_proxy()
            self.expire_date = math.floor(time.time()) + int(expire_date)
            self.proxy = proxy
            request.meta["proxy"] = proxy
            print(self.expire_date, expire_date, proxy)
        else:
            request.meta["proxy"] = self.proxy

    def process_response(self, request, response, spider):
        """对返回的response处理"""
        if response.status == 503 and request.meta.get('retry_times', 0) < 5:       # 503是被封，不是网站崩了
            request.meta['retry_times'] = request.meta.get('retry_times', 0) + 1
            proxy, expire_date = get_proxy()
            self.expire_date = math.floor(time.time()) + int(expire_date)
            self.proxy = proxy
            request.meta["proxy"] = proxy
            return request
        if request.meta.get('retry_times', 0) >= 5:
            self.process_exception(request, '重试次数达到上限: ' + str(response.status), spider)
        return response

    def process_exception(self, request, exception, spider):
        with open('error.txt', 'a') as f:
            if isinstance(exception, str):
                f.write(f"{request.url}--{exception}\n")
            else:
                f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{request.url}--{exception.__repr__()}--process_exception\n")
            f.flush()
'''

class ProxyMiddleware(object):
    # 代理服务器
    proxyServer = "http://http-dyn.abuyun.com:9020"

    # 代理隧道验证信息
    proxyUser = "HR58I089C2G8N57D"
    proxyPass = "3EB3D83115080960"

    # for Python3
    proxy_auth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")

    def process_request(self, request, spider):
        request.meta["proxy"] = self.proxyServer
        request.headers["Proxy-Authorization"] = self.proxy_auth

    def process_response(self, request, response, spider):
        """对返回的response处理"""
        if request.meta.get('retry_times', 0) >= 5:
            self.process_exception(request, '重试次数达到上限', spider)
        return response

    def process_exception(self, request, exception, spider):
        with open('error.txt', 'a') as f:
            if isinstance(exception, str):
                f.write(f"{request.url}--{exception}\n")
            else:
                f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{request.url}--{exception.__repr__()}--process_exception\n")
            f.flush()

