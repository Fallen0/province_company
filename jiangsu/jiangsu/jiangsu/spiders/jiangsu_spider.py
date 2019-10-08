# -*- coding: utf-8 -*-
import datetime
import time
import json
import scrapy
from scrapy import signals
from ..items import AptitudeItem, BeiAnItem, CompanyItem


class JiangsuSpiderSpider(scrapy.Spider):
    name = 'jiangsu_spider'
    allowed_domains = ['221.226.118.170']
    post_url = 'http://221.226.118.170:8080/entpcertlist/queryEntpCertList'
    data = {
        'entpname': '公司',
        'certcode': '',
        'status': '有效',
        'startdate': '',
        'enddate': '',
        'page': '1',
        'rows': '50'
    }
    data_aptitude = {
        'page': '1',
        'rows': '50'
    }

    @staticmethod
    def get_province_company_id():
        return 'jiangsu_' + str(int(time.time() * 1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("now_num", 0)
        crawler.stats.set_value("total_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def start_requests(self):
        yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse, meta={'page': 1},
                                 dont_filter=True)

    def parse(self, response):
        response_json = json.loads(response.text)
        if response_json.get('success') is True:
            # 是第一页yield 所有页
            if response.meta.get('page') == 1:
                # 记录公共有多条
                self.crawler.stats.inc_value('total_num', count=int(response_json.get('pager').get('total')))
                pages = response_json.get('pager').get('maxPage')
                for page in range(2, int(pages) + 1):
                    self.data['page'] = str(page)
                    yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse, priority=2, dont_filter=True)

            # 解析每一页，并发送资质请求
            for company in response_json.get('rows'):
                # 基础信息
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = company.get('entpname')  # 公司名称
                yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                  area_code=320000, source='江苏', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
                self.crawler.stats.inc_value('now_num')
                print('现在：', self.crawler.stats.get_value('now_num'))

                aptitude_organ = company.get('orgname')  # 发证机关
                aptitude_endtime = company.get('validdate')  # 到期时间
                aptitude_id = company.get('certcode')  # 证书编号
                entpcode = company.get('entpcode')  # 资质url用
                url = 'http://221.226.118.170:8080/entpcertlist/queryQualType/' + entpcode + '/' + aptitude_id
                yield scrapy.FormRequest(url, formdata=self.data_aptitude, callback=self.aptitude_parse,
                                         meta={'province_company_id': province_company_id,
                                               'company_name': company_name,
                                               'aptitude_organ': aptitude_organ,
                                               'aptitude_endtime': aptitude_endtime,
                                               'aptitude_id': aptitude_id},
                                         dont_filter=True)

        else:
            with open('error.txt', 'a') as f:
                f.write(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.status}--{response.url}--{response.text}\n")
                f.flush()

    def aptitude_parse(self, response):
        response_json = json.loads(response.text)
        if response_json.get('success') is True:
            time_now = self.get_create_time()  # 生成此刻的时间戳
            for aptitude in response_json.get('rows'):
                aptitude_large = aptitude.get('tradename')  # 资质大类
                aptitude_small = aptitude.get('majorname')  # 资质小类
                level = aptitude.get('levelname')  # 资质等级
                aptitude_startime = aptitude.get('approvedate').split(' ')[0]  # 批准时间/开始时间
                yield AptitudeItem(aptitude_large=aptitude_large, aptitude_small=aptitude_small,
                                   level=level, aptitude_startime=aptitude_startime,
                                   province_company_id=response.meta['province_company_id'],
                                   company_name=response.meta['company_name'],
                                   aptitude_organ=response.meta['aptitude_organ'],
                                   aptitude_endtime=response.meta['aptitude_endtime'].split(' ')[0],
                                   aptitude_id=response.meta['aptitude_id'],
                                   source='江苏', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

        else:
            with open('error.txt', 'a') as f:
                f.write(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.status}--{response.url}--{response.text}\n")
                f.flush()



