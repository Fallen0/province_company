# -*- coding: utf-8 -*-
import datetime
import time
import scrapy
from scrapy import signals

from ..items import CompanyItem, BeiAnItem, AptitudeItem


class HubeiSpiderSpider(scrapy.Spider):
    name = 'hubei_spider'
    allowed_domains = ['jg.hbcic.net.cn']
    start_urls = ['http://jg.hbcic.net.cn/web/QyManage/QyList.aspx']

    data = {
        '__EVENTTARGET': 'lbtnGo',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__EVENTVALIDATION': None,
        'txtQymc': '',
        'txtWydm': '',
        'txtPageIndex': None,
        'hfQylx': ''
    }

    @staticmethod
    def get_province_company_id():
        return 'hubei-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("sn_num", 0)
        crawler.stats.set_value("sw_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def parse(self, response):
        __VIEWSTATE = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
        __EVENTVALIDATION = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()

        # 获取企业详情的url
        hrefs = response.xpath('//table[@filterid="filter"]//tr/td/a/@href').getall()
        for href in hrefs:
            if len(href) > 20 and 'QyInfo.aspx' in href:
                yield scrapy.Request(response.urljoin(href), callback=self.parse_detail)

        # 提取所有页数
        if self.data['txtPageIndex'] is None:
            total_page = str(response.xpath('//*[@id="labPageCount"]/text()').get()).strip()
            self.data['__VIEWSTATE'] = __VIEWSTATE
            self.data['__EVENTVALIDATION'] = __EVENTVALIDATION
            for page in range(2, int(total_page)+1):
                self.data['txtPageIndex'] = str(page)
                print('page:', page)
                yield scrapy.FormRequest('http://jg.hbcic.net.cn/web/QyManage/QyList.aspx', formdata=self.data, callback=self.parse, priority=1)

    def parse_detail(self, response):
        if '省外企业' in response.text:
            # 外省企业
            company_name = str(response.xpath('//*[@id="QYMC"]/text()').get()).strip()
            social_credit_code = str(response.xpath('//*[@id="YYZZBH"]/text()').get()).strip()
            time_now = self.get_create_time()  # 生成此刻的时间戳
            yield BeiAnItem(company_name=company_name, social_credit_code=social_credit_code, record_province='湖北',
                            status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sw_num')
            print('省外：', self.crawler.stats.get_value('sw_num'))
        else:
            # 本省企业基本信息
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(response.xpath('//*[@id="QYMC"]/text()').get()).strip()      # 企业名称
            business_address = str(response.xpath('//*[@id="QYDZ"]/text()').get()).strip()      # 详细地址
            social_credit_code = str(response.xpath('//*[@id="YYZZBH"]/text()').get()).strip()      # 详细地址
            regis_type = str(response.xpath('//*[@id="QYLX"]/text()').get()).strip()      # 经济性质/登记类型
            build_date = str(response.xpath('//*[@id="clsj"]/text()').get()).strip()      # 成立时间
            registered_capital = str(response.xpath('//*[@id="ZCJBJ"]/text()').get()).strip()      # 注册资本金
            leal_person = str(response.xpath('//*[@id="FDDBR"]/text()').get()).strip()      # 法定代表人
            leal_person_duty = str(response.xpath('//*[@id="FDDBR_ZW"]/text()').get()).strip()      # 法定代表人职务
            leal_person_title = str(response.xpath('//*[@id="FDDBR_ZC"]/text()').get()).strip()      # 法定代表人职称
            ceoname = str(response.xpath('//*[@id="QYFZR"]/text()').get()).strip()      # 企业负责人
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              business_address=business_address, social_credit_code=social_credit_code,
                              regis_type=regis_type, build_date=build_date,
                              registered_capital=registered_capital, leal_person=leal_person,
                              leal_person_duty=leal_person_duty, leal_person_title=leal_person_title,
                              ceoname=ceoname, url=response.url,
                              area_code=420000, source='湖北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            # 本省企业资质信息
            tables = response.xpath('//table//table')
            for table in tables:
                aptitude_msgs = table.xpath('.//tr')
                for i, aptitude_msg in enumerate(aptitude_msgs):
                    if i == 0:
                        continue
                    aptitude_id = str(aptitude_msg.xpath('./td[1]/text()').get()).strip()      # 证书编号
                    aptitude_name = str(aptitude_msg.xpath('./td[2]/text()').get()).strip()      # 资质名称
                    level = str(aptitude_msg.xpath('./td[3]/text()').get()).strip()      # 等级
                    aptitude_startime = str(aptitude_msg.xpath('./td[4]/text()').get()).strip()      # 发证日期
                    if '无' in aptitude_startime:
                        aptitude_startime = None
                    aptitude_endtime = str(aptitude_msg.xpath('./td[5]/text()').get()).strip()      # 证书有效截止日期
                    if '无' in aptitude_endtime:
                        aptitude_endtime = None
                    aptitude_organ = str(aptitude_msg.xpath('./td[6]/text()').get()).strip()      # 发证机关
                    yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                                       aptitude_id=aptitude_id, aptitude_name=aptitude_name,
                                       level=level, aptitude_startime=aptitude_startime,
                                       aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                                       source='湖北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')
            print('省内：', self.crawler.stats.get_value('sn_num'))


