# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import logging

from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError

from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.python import global_object_name
import base64
import math
import time
import datetime
import pymongo
import requests
from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from twisted.web._newclient import ResponseFailed, ResponseNeverReceived

logger = logging.getLogger(__name__)


class ExceptionSpiderMiddleware:
    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        with open('error.txt', 'a') as f:
            f.write(
                f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.status}--{response.url}--{exception.__repr__()}\n")
            f.flush()
            print()


def get_proxy():
    res = requests.get('https://dps.kdlapi.com/api/getdps/?orderid=966404044351881&num=1&pt=1&f_et=1&format=json&sep=1')
    if res.json()['code'] != 0:
        return get_proxy()
    proxy = res.json()['data']['proxy_list'][0]
    proxy, expire_date = proxy.split(',')[0], proxy.split(',')[1]
    return 'http://zhao_tai_yu:7av2i9t5@' + proxy, expire_date


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
        if request.meta.get('retry_times', 0) >= 20:
            self.process_exception(request, '重试次数达到上限', spider)
        return response

    def process_exception(self, request, exception, spider):
        with open('error.txt', 'a') as f:
            if isinstance(exception, str):
                f.write(f"{request.url}--{exception}\n")
            else:
                f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{request.url}--{exception.__repr__()}--process_exception\n")
            f.flush()


class FilterDataDownloadMiddleware(object):
    connection = pymongo.MongoClient('119.3.206.20', 27017)
    db = connection['tianjin']  # ['MONGODB_DB']
    collection = db['tianjin_company_name']  # settings['MONGODB_COLLECTION']
    url = set(msg.get('url') for msg in collection.find() if 'url' in msg)
    print(url)
    connection.close()

    def process_request(self, request, spider):
        if request.url in self.url:
            raise IgnoreRequest("过滤已重复url")

    def process_response(self, request, response, spider):
        return response


class MyRetryMiddleware(object):
    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError, ResponseNeverReceived, ResponseFailed)

    def __init__(self, settings):
        if not settings.getbool('RETRY_ENABLED'):
            raise NotConfigured
        self.max_retry_times = settings.getint('RETRY_TIMES')
        self.retry_http_codes = set(int(x) for x in settings.getlist('RETRY_HTTP_CODES'))
        self.priority_adjust = settings.getint('RETRY_PRIORITY_ADJUST')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_response(self, request, response, spider):
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        retry_times = self.max_retry_times

        if 'max_retry_times' in request.meta:
            retry_times = request.meta['max_retry_times']

        stats = spider.crawler.stats
        if retries <= retry_times:
            logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust

            if isinstance(reason, Exception):
                reason = global_object_name(reason.__class__)

            stats.inc_value('retry/count')
            stats.inc_value('retry/reason_count/%s' % reason)
            with open('error.txt', 'a') as f:
                f.write(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{request.url}--{reason}--MyRetry\n")
            return retryreq
        else:
            stats.inc_value('retry/max_reached')
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
