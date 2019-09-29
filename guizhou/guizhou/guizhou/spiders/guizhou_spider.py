# -*- coding: utf-8 -*-
import datetime
import re
import json
import time

import scrapy
import requests
from scrapy import signals

from ..items import BeiAnItem, CompanyItem, AptitudeItem


class GuizhouSpiderSpider(scrapy.Spider):
    name = 'guizhou_spider'
    allowed_domains = ['202.98.194.171']
    start_urls = ['http://202.98.194.171:8088/gzzhxt']
    post_url = 'http://202.98.194.171:8088/gzzhxt/MHWeb/HttpServer/HttpHandler.aspx'
    detail_msg_post_url = 'http://202.98.194.171:8088/GZZHXT/XHTWeb/HttpServer/HttpHandler.aspx'
    _tk = None
    data_page_list = {
        '_opType': 'getPageData_lt',
        '_fromSite': '1',
        'tk': None,
        '_lstID': 'enterpriseLib_Box',
        '_xml': '../../MHWeb/enterpriseLib/LstXml/enterprise_List.xml',
        '_auvOpType': '',
        '_urlParam': '',
        '_pageIndex': None,
        '_pageSize': '99',
        '_sortCol': '0',
        '_baseCondition': ''
    }
    data_company_basic = {
        '_opType': '',
        '_handler': 'XHTMH_WEBBLL.MainPage.MainPageBLL',
        '_method': 'Ajax_GetEnterpriseDataInfo',
        '_fromSite': '1',
        'tk': None,
        'xhtCode': None,
        'ID': ''
    }
    data_company_aptitude = {
        '_opType': 'init_lt',
        '_handler': 'XHTMH_WEBBLL.MainPage.MainPageBLL',
        '_fromSite': '1',
        'tk': None,
        '_lstID': 'enterTabContent1g',
        '_xml': '../../MHWeb/enterpriseLib/LstXml/CorpCertinfo_List.xml',
        '_auvOpType': '',
        '_urlParam': None,
        '_pageIndex': '1',
        '_pageSize': '20',
        '_sortCol': '0',
        '_showQueryPanel': '0',
        '_baseCondition': None
    }

    @property
    def tk(self):
        if self._tk is None:
            data = {'COMMAND': 'INIT'}
            headers = {
                'Origin': 'http://202.98.194.171:8088',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'Accept-Encoding': 'gzip, deflate',
                'Accept-Language': 'zh-CN,zh;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
            }
            response = requests.post(self.post_url, headers=headers, data=data)
            self._tk = re.search(", ?'(.*?)'", response.text).group(1)
            print('tk已过期，新tk:', self._tk)
        return self._tk

    @staticmethod
    def get_province_company_id():
        return 'guizhou-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("all_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def start_requests(self):
        data = {'COMMAND': 'INIT'}
        yield scrapy.FormRequest(self.post_url, formdata=data, callback=self.post_company_list)

    def post_company_list(self, response):
        self._tk = re.search(", ?'(.*?)'", response.text).group(1)
        self.data_page_list['tk'] = self.tk
        self.data_page_list['_pageIndex'] = '1'
        yield scrapy.FormRequest(self.post_url, formdata=self.data_page_list, callback=self.parse_company_list)

    def parse_company_list(self, response):
        json_data = json.loads(response.text.split('|')[1])
        page_index = json_data['pageIndex']
        pages = json_data['pages']
        print(page_index, pages)

        # 解析企业唯一id: xht_code
        self.data_company_basic['tk'] = self.tk
        for msg in json_data.get('rows', []):
            row_onclick = msg.get('rowOnclick', None)
            if row_onclick:
                xht_code = re.search("Info\('(.*?)'", row_onclick).group(1)
                self.data_company_basic['xhtCode'] = xht_code
                yield scrapy.FormRequest(self.detail_msg_post_url, formdata=self.data_company_basic,
                                         callback=self.post_aptitude, meta={'xhtCode': xht_code}, priority=1)

        # 如果是第一页，yield所有页数
        if page_index == 1:
            for i in range(2, int(pages)+1):
                self.data_page_list['_pageIndex'] = str(i)
                yield scrapy.FormRequest(self.post_url, formdata=self.data_page_list, callback=self.parse_company_list, priority=3)

    def post_aptitude(self, response):
        self.data_company_aptitude['tk'] = self.tk
        self.data_company_aptitude['_urlParam'] = 'xhtCode=' + response.meta['xhtCode']
        self.data_company_aptitude['_baseCondition'] = 'xhtCode=' + response.meta['xhtCode'] + '%c0_ID='
        yield scrapy.FormRequest(self.detail_msg_post_url, formdata=self.data_company_aptitude,
                                 callback=self.parse_enter_pipeline, meta={'response': response.text}, priority=2)

    def parse_enter_pipeline(self, response):
        company_json_data = json.loads(response.meta['response'].split('|')[1])
        # 解析企业基础信息
        province_company_id = self.get_province_company_id()  # 生成此刻的企业id
        time_now = self.get_create_time()  # 生成此刻的时间戳
        business_address = company_json_data.get('data')[0].get('Address')     # 经营地址
        build_date = company_json_data.get('data')[0].get('CorpBirthDate')     # 成立日期
        social_credit_code = company_json_data.get('data')[0].get('CorpCode')     # 社会统一信用码
        company_name = company_json_data.get('data')[0].get('CorpName')     # 企业名称
        leal_person = company_json_data.get('data')[0].get('LegalMan')     # 企业法定代表人
        url = 'http://202.98.194.171:8088/GZZHXT/MHWeb/enterpriseLib/enterprise_Info.html?xhtCode=' + company_json_data.get('data')[0].get('xhtCorpID')     # url
        # print(contact_address, build_date, social_credit_code, name, leal_person)
        yield CompanyItem(province_company_id=province_company_id, business_address=business_address,
                          build_date=build_date, social_credit_code=social_credit_code,
                          company_name=company_name, leal_person=leal_person,
                          url=url,
                          area_code=520000, source='贵州', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        # 获取资质信息
        json_data = json.loads(response.text.split('|')[1])
        print(json_data.get('rows'))
        if json_data.get('rows'):
            for num in json_data.get('rows'):
                if num.get('columns')[7].get('content') == '有效':   # 证书是否有效，不是有效就丢弃
                    aptitude_id = num.get('columns')[2].get('content')   # 证书编号
                    # tech_lead = num.get('columns')[3].get('content')   # 技术负责人
                    # contact_person = num.get('columns')[4].get('content')   # 企业负责人
                    aptitude_organ = num.get('columns')[5].get('content')   # 发证机关
                    aptitude_startime = num.get('columns')[6].get('content')   # 发证日期
                    yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                                       aptitude_id=aptitude_id, aptitude_organ=aptitude_organ,
                                       aptitude_startime=aptitude_startime,
                                       source='贵州', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

        self.crawler.stats.inc_value('all_num')
        print(self.crawler.stats.get_value('all_num'))

