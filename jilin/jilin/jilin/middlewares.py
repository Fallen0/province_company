# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import base64
import time
import datetime
import requests
import math
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
            f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.url}--{exception.__repr__()}\n")
            f.flush()
            print()


def get_proxy():
    res = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=966404044351881&num=1&pt=1&f_et=1&format=json&sep=1')
    if res.json()['code'] != 0:
        return get_proxy()
    proxy = res.json()['data']['proxy_list'][0]
    proxy, expire_date = proxy.split(',')[0], proxy.split(',')[1]
    status_code = verify_proxy('http://zhao_tai_yu:7av2i9t5@' + proxy)
    if status_code == 403:
        return get_proxy()
    return 'http://zhao_tai_yu:7av2i9t5@' + proxy, expire_date


def verify_proxy(proxy):
    headers = {
        'Proxy-Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    res = requests.get('http://cx.jlsjsxxw.com/corpinfo/CorpInfo.aspx#', headers=headers, proxies={'http': proxy})
    print('代理响应码：', res.status_code)    # 快代理可能是403
    return res.status_code


class ProxyMiddleware(object):
    expire_date = 0
    proxy = None

    def process_request(self, request, spider):
        if self.expire_date < math.floor(time.time()) + 10:
            proxy, expire_date = get_proxy()
            self.expire_date = math.floor(time.time()) + int(expire_date)
            self.proxy = proxy
            request.meta["proxy"] = proxy
            print(self.expire_date, expire_date, proxy)
        else:
            request.meta["proxy"] = self.proxy

    def process_response(self, request, response, spider):
        """对返回的response处理"""
        if response.status == 403:
            proxy, expire_date = get_proxy()
            self.expire_date = math.floor(time.time()) + int(expire_date)
            self.proxy = proxy
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

