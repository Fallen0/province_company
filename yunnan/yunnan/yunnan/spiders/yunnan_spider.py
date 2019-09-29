# -*- coding: utf-8 -*-
import datetime
import re
import time
import math
import scrapy
from scrapy import signals
from ..items import CompanyItem, BeiAnItem, AptitudeItem


class YunnanSpiderSpider(scrapy.Spider):
    name = 'yunnan_spider'
    allowed_domains = ['220.163.15.148']
    get_url = 'http://220.163.15.148/InfoQuery/EnterpriseList?page='

    @staticmethod
    def get_province_company_id():
        return 'yunnan-' + str(int(time.time() * 1000))

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
        # 全信息列表
        yield scrapy.Request('http://220.163.15.148/InfoQuery/EnterpriseList')
        # error_test
        # yield scrapy.Request('http://220.163.15.148/InfoQuery/EnterpriseDetial?keyValue=c90bd0c8-54ee-4a58-884d-0c61d2447a74', callback=self.parse_detail)

    def parse(self, response):
        total_pairs_word = response.xpath('//div[@class="jump fl"]/span[1]').get()
        total_pairs = re.search('(\d+)', total_pairs_word).group(1)
        print(total_pairs, math.ceil(int(total_pairs)/15) + 1, response.url)
        if 'EnterpriseList' in response.url and 'page' not in response.url:
            for pair in range(2, math.ceil(int(total_pairs)/15) + 1):
                yield scrapy.Request(self.get_url+str(pair), callback=self.parse, priority=1)

        hrefs = response.xpath('//table//tr/td[2]/a/@href').getall()
        for href in hrefs:
            yield scrapy.Request(response.urljoin(href), callback=self.parse_detail)

    def parse_detail(self, response):
        # 基础信息
        province_company_id = self.get_province_company_id()  # 生成此刻的企业id
        time_now = self.get_create_time()  # 生成此刻的时间戳
        company_name = ''.join(response.xpath('//div[@class="tLayer-1"]//h3/text()').getall())
        x = list(map(lambda x: re.sub("\s", "", x), company_name))
        company_name = ''.join(x)
        social_credit_code = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[1]/td[2]/text()').get()).strip()  # 营业执照号
        if not social_credit_code or social_credit_code == 'None':
            social_credit_code = str(
                response.xpath('//div[@class="tLayer-1"]//table//tr[4]/td[2]/text()').get()).strip()  # 组织机构代码
        leal_person = str(response.xpath('//div[@class="tLayer-1"]//table//tr[2]/td[2]/text()').get()).strip()  # 法人代表
        regis_type = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[3]/td[2]/text()').get()).strip()  # 企业登记注册类型
        contact_person = str(response.xpath('//div[@class="tLayer-1"]//table//tr[5]/td[2]/text()').get()).strip()  # 联系人
        contact_address = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[6]/td[2]/text()').get()).strip()  # 联系地址
        registered_capital = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[1]/td[4]/text()').get()).strip()  # 注册资金
        if registered_capital and bool(re.search('(\d)', registered_capital)):
            registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
        leal_person_duty = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[2]/td[4]/text()').get()).strip()  # 法人代表职位
        leal_person_title = str(
            response.xpath('//div[@class="tLayer-1"]//table//tr[3]/td[4]/text()').get()).strip()  # 法人代表职称
        build_date = str(response.xpath('//div[@class="tLayer-1"]//table//tr[4]/td[4]/text()').get()).strip()  # 成立时间
        reg_address1 = str(response.xpath('//div[@class="tLayer-1"]//table//tr[5]/td[4]/text()').get()).strip()  # 所属省市
        reg_address2 = str(response.xpath('//div[@class="tLayer-1"]//table//tr[6]/td[4]/text()').get()).strip()  # 所属州市
        if reg_address1 and reg_address1 != 'None':
            regis_address = reg_address1
            if reg_address2 and reg_address2 != 'None':
                regis_address = regis_address + ' ' + reg_address2
        else:
            regis_address = None
        yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                          social_credit_code=social_credit_code, leal_person=leal_person,
                          regis_type=regis_type, contact_person=contact_person,
                          contact_address=contact_address, registered_capital=registered_capital,
                          leal_person_duty=leal_person_duty, leal_person_title=leal_person_title,
                          build_date=build_date, regis_address=regis_address, url=response.url,
                          area_code=530000, source='云南', status=1, create_time=time_now, modification_time=time_now,
                          is_delete=0)
        self.crawler.stats.inc_value('sn_num')
        # 企业资质
        qualifies = response.xpath('//div[@id="Qualifi"]/table//tr')
        for i, qualify in enumerate(qualifies):
            if i == 0:
                continue
            aptitude_type = str(qualify.xpath('./td[2]/text()').get()).strip()  # 资质类型
            aptitude_id = str(qualify.xpath('./td[3]/text()').get()).strip()  # 资质证书编号
            aptitude_name = str(qualify.xpath('./td[4]/text()').get()).strip()  # 资质专业及等级
            aptitude_organ = str(qualify.xpath('./td[5]/text()').get()).strip()  # 发证机关
            aptitude_startime = str(qualify.xpath('./td[6]/text()').get()).strip()  # 发证日期
            aptitude_endtime = str(qualify.xpath('./td[7]/text()').get()).strip()  # 有效期至
            if (aptitude_type and aptitude_type != 'None') or \
                    (aptitude_id and aptitude_id != 'None') or \
                    (aptitude_name and aptitude_name != 'None') or \
                    (aptitude_organ and aptitude_organ != 'None') or \
                    (aptitude_startime and aptitude_startime != 'None') or \
                    (aptitude_endtime and aptitude_endtime != 'None'):
                yield AptitudeItem(province_company_id=province_company_id, aptitude_type=aptitude_type,
                                   aptitude_id=aptitude_id, aptitude_name=aptitude_name,
                                   aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                                   aptitude_endtime=aptitude_endtime, company_name=company_name,
                                   source='云南', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
