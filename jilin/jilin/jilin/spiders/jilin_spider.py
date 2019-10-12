# -*- coding: utf-8 -*-
import datetime
import re
import scrapy
from urllib.parse import urlencode
import time
import json
from lxml import etree
from scrapy import signals

from ..items import CompanyItem, BeiAnItem, AptitudeItem


class JilinSpiderSpider(scrapy.Spider):
    name = 'jilin_spider'
    allowed_domains = ['jlsjsxxw.com']

    @staticmethod
    def get_province_company_id():
        return 'jilin_' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("sn_num", 0)
        crawler.stats.set_value("sw_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def gen_params_sn(self, nPageIndex=1, nPageCount=0, nPageRowsCount=0):
        params = (
            ('method', 'SnCorpData'),  # 'SnCorpData'
            ('CorpName', ''),
            ('QualiType', ''),
            ('TradeID', ''),
            ('BoundID', ''),
            ('LevelID', ''),
            ('CityNum', ''),
            ('nPageIndex', str(nPageIndex)),
            ('nPageCount', str(nPageCount)),
            ('nPageRowsCount', str(nPageRowsCount)),
            ('nPageSize', '20'),
            ('_', str(int(time.time() * 1000))),
        )
        return urlencode(params)

    def gen_params_sw(self, nPageIndex=1, nPageCount=0, nPageRowsCount=0):
        params = (
            ('method', 'SwCorpData'),   # 'SwCorpData'
            ('CorpName', ''),
            ('AptitudeNum', ''),
            ('TradeID', ''),
            ('BoundID', ''),
            ('LevelID', ''),
            ('ProvinceNum', ''),
            ('nPageIndex', str(nPageIndex)),
            ('nPageCount', str(nPageCount)),
            ('nPageRowsCount', str(nPageRowsCount)),
            ('nPageSize', '20'),
            ('_', str(int(time.time() * 1000))),
        )
        return urlencode(params)

    def http_302_error(self, failure):
        with open('error.txt', 'a') as f:
            f.write(f"{failure.request.url}--{failure.__repr__()}--http_302_error\n")
            f.flush()
            print()

    def start_requests(self):
        # 省内全信息列表
        sn_path = self.gen_params_sn()
        sn_url = 'http://cx.jlsjsxxw.com/handle/NewHandler.ashx' + '?' + sn_path
        yield scrapy.Request(sn_url, callback=self.parse_sn, errback=self.http_302_error)
        # 入吉(省外)全信息列表
        sw_path = self.gen_params_sw()
        sw_url = 'http://cx.jlsjsxxw.com/handle/NewHandler.ashx' + '?' + sw_path
        yield scrapy.Request(sw_url, callback=self.parse_sw, errback=self.http_302_error)

        # error_test
        # yield scrapy.Request('http://httpbin.org/get')
        # yield scrapy.Request('http://220.163.15.148/InfoQuery/EnterpriseDetial?keyValue=c90bd0c8-54ee-4a58-884d-0c61d2447a74', callback=self.parse_detail)

    def parse_sn(self, response):
        response_json = json.loads(response.text)
        html_response = response_json.get('tb')
        # 请求所有页数
        if int(response_json.get('nPageIndex')) == 1:
            for i in range(2, int(response_json.get('nPageCount'))+1):
                sn_path = self.gen_params_sn(i,
                                             response_json.get('nPageCount'),
                                             response_json.get('nPageRowsCount'))
                # print(sn_path)
                url = 'http://cx.jlsjsxxw.com/handle/NewHandler.ashx' + '?' + sn_path
                yield scrapy.Request(url, callback=self.parse_sn, priority=1)

        # 解析链接，进入详细页
        if html_response:
            html_response = etree.HTML(html_response)
            hrefs = html_response.xpath('//a/@href')
            for href in hrefs:
                yield scrapy.Request(response.urljoin(href), callback=self.parse_sn_detail)

    def parse_sn_detail(self, response):
        # 省内企业基本信息
        province_company_id = self.get_province_company_id()  # 生成此刻的企业id
        time_now = self.get_create_time()  # 生成此刻的时间戳
        company_name = str(response.xpath('//table[@class="cpd_basic_table"]//tr[1]/td[2]/text()').get()).strip()      # 企业名称
        social_credit_code = str(response.xpath('//table[@class="cpd_basic_table"]//tr[2]/td[2]/text()').get()).strip()      # 营业执照号
        if not social_credit_code or social_credit_code == 'None':
            social_credit_code = str(response.xpath('//table[@class="cpd_basic_table"]//tr[2]/td[6]/text()').get()).strip()      # 组织机构代码
        registered_capital = str(response.xpath('//table[@class="cpd_basic_table"]//tr[2]/td[4]/text()').get()).strip()      # 注册资本金
        if registered_capital and bool(re.search('(\d)', registered_capital)):
            registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
        leal_person = str(response.xpath('//table[@class="cpd_basic_table"]//tr[3]/td[2]/text()').get()).strip()      # 法定代表人
        leal_person_title = str(response.xpath('//table[@class="cpd_basic_table"]//tr[3]/td[4]/text()').get()).strip()      # 法定代表人职称
        leal_person_duty = str(response.xpath('//table[@class="cpd_basic_table"]//tr[3]/td[6]/text()').get()).strip()      # 法定代表人职务
        regis_type = str(response.xpath('//table[@class="cpd_basic_table"]//tr[4]/td[2]/text()').get()).strip()      # 企业经济类型
        build_date = str(response.xpath('//table[@class="cpd_basic_table"]//tr[4]/td[4]/text()').get()).strip()      # 成立时间
        postalcode = str(response.xpath('//table[@class="cpd_basic_table"]//tr[4]/td[6]/text()').get()).strip()      # 邮政编码
        contact_person = str(response.xpath('//table[@class="cpd_basic_table"]//tr[5]/td[2]/text()').get()).strip()      # 联系人
        reg_address1 = str(response.xpath('//table[@class="cpd_basic_table"]//tr[5]/td[4]/text()').get()).strip()      # 所属省
        reg_address2 = str(response.xpath('//table[@class="cpd_basic_table"]//tr[5]/td[6]/text()').get()).strip()      # 所属市
        if reg_address1 and reg_address1 != 'None':
            regis_address = reg_address1
            if reg_address2 and reg_address2 != 'None':
                regis_address = regis_address + ' ' + reg_address2
        else:
            regis_address = None
        contact_address = str(response.xpath('//table[@class="cpd_basic_table"]//tr[6]/td[2]/text()').get()).strip()   # 联系地址
        yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                          social_credit_code=social_credit_code, registered_capital=registered_capital,
                          leal_person=leal_person, leal_person_title=leal_person_title,
                          leal_person_duty=leal_person_duty, regis_type=regis_type,
                          build_date=build_date, postalcode=postalcode,
                          contact_person=contact_person, regis_address=regis_address,
                          contact_address=contact_address, url=response.url,
                          area_code=220000, source='吉林', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        self.crawler.stats.inc_value('sn_num')
        print('省内：', self.crawler.stats.get_value('sn_num'))
        # 资质信息
        types = response.xpath('//*[@class="details_infor_content_01"]/div')
        tables = response.xpath('//*[@class="details_infor_content_01"]/table')
        if len(types) != len(tables):
            raise TypeError('资质信息种类个数和table个数不一致，需要手动检查')
        elif len(types) == len(tables) == 0:
            pass
        else:
            for type1, table in zip(types, tables):
                # mark = str(type1.xpath('./span/text()').get()).strip()      # 建筑业   安全生产许可  。。。
                aptitude_id = str(table.xpath('.//tr[1]/td[2]/text()').get()).strip()     # 资质证书编号
                aptitude_endtime = str(table.xpath('.//tr[1]/td[4]/text()').get()).strip()     # 有效期至
                aptitude_startime = str(table.xpath('.//tr[1]/td[6]/text()').get()).strip()     # 发证日期
                aptitude_organ = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()     # 发证机关
                aptitude_name = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()     # 资质范围
                yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                   aptitude_endtime=aptitude_endtime, aptitude_startime=aptitude_startime,
                                   aptitude_organ=aptitude_organ, aptitude_name=aptitude_name, company_name=company_name,
                                   source='吉林', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    # ----------------------------------------外省解析函数-----------------------------------------
    def parse_sw(self, response):
        response_json = json.loads(response.text)
        html_response = response_json.get('tb')
        # 请求所有页数
        if int(response_json.get('nPageIndex')) == 1:
            for i in range(2, int(response_json.get('nPageCount'))+1):
                sw_path = self.gen_params_sw(i,
                                             response_json.get('nPageCount'),
                                             response_json.get('nPageRowsCount'))
                # print(sw_path)
                url = 'http://cx.jlsjsxxw.com/handle/NewHandler.ashx' + '?' + sw_path
                yield scrapy.Request(url, callback=self.parse_sw, priority=1)

        # 解析企业名称， 注册省份， 备案省份
        if html_response:
            html_response = etree.HTML(html_response)
            trs = html_response.xpath('//tr')
            for tr in trs:
                company_name = tr.xpath('./td[contains(@class, "company_name")]/@title')
                company_name = company_name[0] if company_name else None
                # areaname = tr.xpath('./td[3]/@title')
                # areaname = areaname[0] if areaname else None
                time_now = self.get_create_time()  # 生成此刻的时间戳
                yield BeiAnItem(company_name=company_name, record_province='吉林',
                                status=1, create_time=time_now, modification_time=time_now, is_delete=0)

                self.crawler.stats.inc_value('sw_num')
                print('省外：', self.crawler.stats.get_value('sw_num'))



