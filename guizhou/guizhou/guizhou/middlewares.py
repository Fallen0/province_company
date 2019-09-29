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
            f.write(f"{response.status}--{response.url}--{exception.__repr__()}\n")
            f.flush()
            print()


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


