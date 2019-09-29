# -*- coding: utf-8 -*-
import datetime
import time

import scrapy
import re

from scrapy import signals

from ..items import CompanyItem, AptitudeItem, BeiAnItem


class GansuSpiderSpider(scrapy.Spider):
    name = 'gansu_spider'
    allowed_domains = ['61.178.32.163']
    post_url = 'http://61.178.32.163:84/GSJZJGweb/index.aspx?tabid=1f1e1aa9-6feb-40ed-a063-6f8a27d9ed04'
    post_url_sw = 'http://61.178.32.163:84/GSJZJGweb/index.aspx?tabid=99deb541-340c-455f-858b-d1211036148e'
    sw_codes = {"110000": "北京市",
                "310000": "上海市",
                "120000": "天津市",
                "500000": "重庆市",
                "340000": "安徽省",
                "350000": "福建省",
                "440000": "广东省",
                "450000": "广西壮族自治区",
                "520000": "贵州省",
                "460000": "海南省",
                "130000": "河北省",
                "410000": "河南省",
                "230000": "黑龙江省",
                "420000": "湖北省",
                "430000": "湖南省",
                "220000": "吉林省",
                "320000": "江苏省",
                "360000": "江西省",
                "210000": "辽宁省",
                "150000": "内蒙古自治区",
                "640000": "宁夏回族自治区",
                "630000": "青海省",
                "370000": "山东省",
                "140000": "山西省",
                "610000": "陕西省",
                "510000": "四川省",
                "650000": "新疆维吾尔自治区",
                "540000": "西藏自治区",
                "530000": "云南省",
                "330000": "浙江省", }
    data_sn = {
        'Webb_Upload_Enable': 'False',
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': None,
        'keyword': '站内搜索',
        '_ctl10:tbEnterpriseName': '',
        '_ctl10:dprlRegionCode': '',
        '_ctl10:dropAptitudeType': '',
        '_ctl10:txtAptitudeName': '',
        '_ctl10:btnDemand': ''
    }
    data_sn_next = {
        'Webb_Upload_Enable': 'False',
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__LASTFOCUS': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': None,
        'keyword': '站内搜索',
        '_ctl10:tbEnterpriseName': '',
        '_ctl10:dprlRegionCode': '',
        '_ctl10:dropAptitudeType': '',
        '_ctl10:txtAptitudeName': '',
        '_ctl10:_ctl4': ''
        '_ctl10:_ctl5: GO'
    }
    data_sw = {
        'Webb_Upload_Enable': 'False',
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': None,
        'keyword': '站内搜索',
        '_ctl10:dropAptitudeType': '',
        '_ctl10:tbEnterpriseName': '',
        '_ctl10:dprlRegionCode': None,
        '_ctl10:btnDemand': '',
        '_ctl10:_ctl4': ''
    }
    data_sw_next = {
        'Webb_Upload_Enable': 'False',
        '__EVENTTARGET': '_ctl10$_ctl2',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': None,
        'keyword': '站内搜索',
        '_ctl10:dropAptitudeType': '',
        '_ctl10:tbEnterpriseName': '',
        '_ctl10:dprlRegionCode': None,
        '_ctl10:_ctl4': ''
    }

    @staticmethod
    def get_province_company_id():
        return 'gansu-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("sn_num", 0)
        crawler.stats.set_value("sw_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def start_requests(self):
        # 省内
        yield scrapy.Request('http://61.178.32.163:84/GSJZJGweb/index.aspx?tabid=1f1e1aa9-6feb-40ed-a063-6f8a27d9ed04', callback=self.parse)
        # 省外
        yield scrapy.Request('http://61.178.32.163:84/GSJZJGweb/index.aspx?tabid=99deb541-340c-455f-858b-d1211036148e', callback=self.parse_sw)

    def parse(self, response):
        # 省内企业，post站内搜索，只用一次
        __VIEWSTATE = response.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
        __VIEWSTATEGENERATOR = response.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
        __EVENTVALIDATION = response.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
        self.data_sn['__VIEWSTATE'] = __VIEWSTATE
        self.data_sn['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
        self.data_sn['__EVENTVALIDATION'] = __EVENTVALIDATION
        yield scrapy.FormRequest(self.post_url, formdata=self.data_sn, callback=self.parse_sn_list, meta={'first_page': True})

    def parse_sn_list(self, response):
        # 省内企业获取所有页
        if response.meta.get('first_page', False):
            page = response.xpath('//*[@id="_ctl10_Paging"]/span/font[2]/text()').get().strip()
            print(page)
            for p in range(2, int(page)+1):
                __VIEWSTATE = response.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
                __VIEWSTATEGENERATOR = response.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
                __EVENTVALIDATION = response.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
                self.data_sn_next['__VIEWSTATE'] = __VIEWSTATE
                self.data_sn_next['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
                self.data_sn_next['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data_sn_next['_ctl10:_ctl4'] = str(p)
                yield scrapy.FormRequest(self.post_url, formdata=self.data_sn_next, callback=self.parse_sn_list)

        hrefs = response.xpath('//table//a/@onclick').getall()
        for href in hrefs:
            url = re.search("window\.open\('(.*?)'", href).group(1)
            yield scrapy.Request(response.urljoin(url), callback=self.parse_sn_detail)

    def parse_sn_detail(self, response):
        province_company_id = self.get_province_company_id()  # 生成此刻的企业id
        time_now = self.get_create_time()  # 生成此刻的时间戳
        company_name = str(response.xpath('//span[@id="lblEnterpriseName"]/text()').get()).strip()  # 企业名称
        business_address = str(response.xpath('//span[@id="lblAddress"]/text()').get()).strip()  # 企业地址
        leal_person = str(response.xpath('//span[@id="lblCorporationName"]/text()').get()).strip()  # 法定代表人
        registered_capital = str(response.xpath('//span[@id="lblRegisterFund"]/text()').get()).strip()  # 注册资本
        regis_type = str(response.xpath('//span[@id="lblEconomyKind"]/text()').get()).strip()  # 经济性质
        social_credit_code = str(response.xpath('//span[@id="lblLicenceNum"]/text()').get()).strip()  # 营业执照注册号
        yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                          business_address=business_address, leal_person=leal_person,
                          registered_capital=registered_capital, regis_type=regis_type,
                          social_credit_code=social_credit_code, url=response.url,
                          area_code=620000, source='甘肃', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

        aptitudes = response.xpath('//tr[@id="trMainAptitude"]//tr/td/text()').getall()
        for aptitude in aptitudes:
            aptitude_list = aptitude.strip().split(' ')
            aptitude_id = ''
            level = ''
            aptitude_large = ''
            aptitude_small = ''
            for i, aptitude_detail in enumerate(aptitude_list[::-1]):
                if '*' in aptitude_detail or bool(re.search('(\d)', aptitude_detail)) or aptitude_detail == '无':
                    aptitude_id = aptitude_detail  # 资质证书编号
                elif i == 0:
                    level = aptitude_detail  # 资质等级
                elif i == 1:
                    aptitude_large = aptitude_detail  # 资质大类
                elif i == 2:
                    aptitude_small = aptitude_detail  # 资质小类
                elif i > 2:
                    with open('error.txt', 'a') as f:
                        f.write(f"{response.url}--有长度超过4的企业资质信息\n")
                        f.flush()
            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                               level=level, aptitude_large=aptitude_large,
                               aptitude_small=aptitude_small, company_name=company_name,
                               source='甘肃', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        self.crawler.stats.inc_value('sn_num')

    def parse_sw(self, response):
        # 省外企业，post站内搜索，只用一次
        __VIEWSTATE = response.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
        __VIEWSTATEGENERATOR = response.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
        __EVENTVALIDATION = response.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
        self.data_sw['__VIEWSTATE'] = __VIEWSTATE
        self.data_sw['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
        self.data_sw['__EVENTVALIDATION'] = __EVENTVALIDATION
        for sw_code in self.sw_codes.keys():
            self.data_sw['_ctl10:dprlRegionCode'] = sw_code
            yield scrapy.FormRequest(self.post_url_sw, formdata=self.data_sw, callback=self.parse_sw_list,
                                     meta={'province': sw_code})

    def parse_sw_list(self, response):
        company_list = response.xpath('//table[@id="_ctl10_gvList"]//tr/td[1]/a/text()').getall()
        for company in company_list:
            company_name = company.strip()
            time_now = self.get_create_time()  # 生成此刻的时间戳
            yield BeiAnItem(company_name=company_name, record_province='甘肃',
                            status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sw_num')

        # 判断是否有下一页
        onclick_attributes = response.xpath('//*[@id="_ctl10_Paging"]//a/@href').getall()
        for onclick_attribute in onclick_attributes:
            if 'ctl2' in onclick_attribute:
                __VIEWSTATE = response.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
                __VIEWSTATEGENERATOR = response.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
                __EVENTVALIDATION = response.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
                self.data_sw_next['__VIEWSTATE'] = __VIEWSTATE
                self.data_sw_next['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
                self.data_sw_next['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data_sw_next['_ctl10:dprlRegionCode'] = response.meta['province']
                yield scrapy.FormRequest(self.post_url_sw, formdata=self.data_sw_next, callback=self.parse_sw_list,
                                         meta={'province': response.meta['province']})
                break
