# -*- coding: utf-8 -*-
import datetime
import time

import scrapy
import re

from scrapy import signals

from ..items import CompanyItem, BeiAnItem, AptitudeItem


class HeibeiSpiderSpider(scrapy.Spider):
    name = 'heibei_spider'
    allowed_domains = ['zfcxjst.hebei.gov.cn']

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("sn_num", 0)
        crawler.stats.set_value("sw_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    def start_requests(self):
        # 建筑
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=264649', callback=self.parse)
        # 勘察
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=290807', callback=self.parse_reconnaissance)
        # 设计
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=278453', callback=self.parse_design)
        # 安全生产许可证
        # yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=273505', callback=self.parse_security)
        # 建设工程质量检测机构
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=250510', callback=self.parse_quality)
        # 工程监理企业
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=219809', callback=self.parse_supervisor)
        # 备案
        yield scrapy.Request('http://zfcxjst.hebei.gov.cn/was5/web/search?channelid=289933', callback=self.parse_in_hebei)

    @staticmethod
    def get_province_company_id():
        return 'hebei-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def parse(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            time_now = self.get_create_time()
            province_company_id = self.get_province_company_id()
            aptitude_id = tr.xpath('./td[1]/text()').get()  # 证书编号
            aptitude_type = tr.xpath('./td[2]/text()').get()  # 资质类型
            company_name = tr.xpath('./td[3]/text()').get()  # 企业名称
            aptitude_large = tr.xpath('./td[4]/text()').get()  # 资质资格序列/资质大类
            aptitude_small = tr.xpath('./td[5]/text()').get()  # 资质小类
            level = tr.xpath('./td[6]/text()').get()  # 资质等级
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              source='河北', area_code=130000, status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            time_now = self.get_create_time()
            yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                               aptitude_id=aptitude_id, aptitude_type=aptitude_type,
                               aptitude_large=aptitude_large, aptitude_small=aptitude_small,
                               level=level,
                               source='河北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')

        # 建筑是第一页，就去找最后一页
        if 'page' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=264649&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse)

    def parse_reconnaissance(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            time_now = self.get_create_time()
            province_company_id = self.get_province_company_id()
            aptitude_id = tr.xpath('./td[1]/text()').get()  # 证书编号
            company_name = tr.xpath('./td[2]/text()').get()  # 企业名称
            aptitude_large = tr.xpath('./td[3]/text()').get()  # 资质资格序列/资质大类
            aptitude_small = tr.xpath('./td[4]/text()').get()  # 资质小类
            level = tr.xpath('./td[5]/text()').get()  # 资质等级
            aptitude_usefultime = tr.xpath('./td[6]/text()').get()  # 证书有效期
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              source='河北', area_code=130000, status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            time_now = self.get_create_time()
            yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                               aptitude_id=aptitude_id,
                               aptitude_large=aptitude_large, aptitude_small=aptitude_small,
                               level=level, aptitude_usefultime=aptitude_usefultime,
                               source='河北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')

        # 勘察是第一页，就去找最后一页
        if 'page' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=290807&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_reconnaissance)

    def parse_design(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            time_now = self.get_create_time()
            province_company_id = self.get_province_company_id()
            aptitude_id = tr.xpath('./td[1]/text()').get()  # 证书编号
            company_name = tr.xpath('./td[2]/text()').get()  # 企业名称
            aptitude_large = tr.xpath('./td[3]/text()').get()  # 资质资格序列/资质大类
            aptitude_small = tr.xpath('./td[4]/text()').get()  # 资质小类
            level = tr.xpath('./td[5]/text()').get()  # 资质等级
            aptitude_usefultime = tr.xpath('./td[6]/text()').get()  # 证书有效期
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              source='河北', area_code=130000, status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            time_now = self.get_create_time()
            yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                               aptitude_id=aptitude_id,
                               aptitude_large=aptitude_large, aptitude_small=aptitude_small,
                               level=level, aptitude_usefultime=aptitude_usefultime,
                               source='河北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')

        # 设计是第一页，就去找最后一页
        if 'page' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=278453&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_design)

    def parse_security(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            time_now = self.get_create_time()
            province_company_id = self.get_province_company_id()
            # department = tr.xpath('./td[1]/text()').get()  # 所属城市
            aptitude_id = tr.xpath('./td[2]/text()').get()  # 证书编号
            company_name = tr.xpath('./td[3]/text()').get()  # 企业名称
            leal_person = tr.xpath('./td[4]/text()').get()  # 法定代表人
            aptitude_endtime = tr.xpath('./td[5]/text()').get()  # 有效期至
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              leal_person=leal_person,
                              source='河北', area_code=130000, status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            time_now = self.get_create_time()
            yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                               aptitude_id=aptitude_id, aptitude_endtime=aptitude_endtime,
                               source='河北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')

        # 安全是第一页，就去找最后一页
        if 'page' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=273505&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_security)

    def parse_quality(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            detail_href = tr.xpath('./td[6]/a/@href').get()
            yield scrapy.Request(response.urljoin(detail_href), callback=self.parse_quality_detail)

        # 质量检测是第一页，就去找最后一页
        if 'page' not in response.url and 'record' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=250510&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_quality)

    def parse_quality_detail(self, response):
        province_company_id = self.get_province_company_id()
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                company_name = tr.xpath('./td[2]/text()').get()  # 企业名称
                # department = tr.xpath('./td[4]/text()').get()  # 所属城市
            elif i == 1:
                leal_person = tr.xpath('./td[2]/text()').get()  # 法定代表人
                aptitude_id = tr.xpath('./td[4]/text()').get()  # 证书编号
            elif i == 2:
                aptitude_endtime = tr.xpath('./td[2]/text()').get()  # 证书有效期至
            elif i == 3:
                aptitude_name = tr.xpath('./td[2]/text()').get()  # 检测资质范围/资质项名称
                if aptitude_name:
                    x = list(map(lambda x: re.sub("\s", "", x), aptitude_name))
                    aptitude_name = ''.join(x)
        print(aptitude_id, company_name, leal_person, aptitude_endtime, aptitude_name)
        # if '250510' in response.url:
        #     mark = '建设工程质量检测机构企业'
        # elif '219809' in response.url:
        #     mark = '工程监理企业'
        time_now = self.get_create_time()
        yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                          leal_person=leal_person,
                          source='河北', area_code=130000, status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        yield AptitudeItem(province_company_id=province_company_id, company_name=company_name,
                           aptitude_id=aptitude_id, aptitude_endtime=aptitude_endtime,
                           aptitude_name=aptitude_name,
                           source='河北', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        self.crawler.stats.inc_value('sn_num')

    def parse_supervisor(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            detail_href = tr.xpath('./td[6]/a/@href').get()
            yield scrapy.Request(response.urljoin(detail_href), callback=self.parse_supervisor_detail)

        # 是第一页，就去找最后一页
        if 'page' not in response.url and 'record' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=219809&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_supervisor)

    # 监理企业和质量检测机构结构一样
    parse_supervisor_detail = parse_quality_detail

    # 备案解析
    def parse_in_hebei(self, response):
        trs = response.xpath('//table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            detail_href = tr.xpath('./td[4]/a/@href').get()
            yield scrapy.Request(response.urljoin(detail_href), callback=self.parse_in_hebei_detail)

        # 是第一页，就去找最后一页
        if 'page' not in response.url and 'record' not in response.url:
            last_page_href = response.xpath('//a[@class="last-page"]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page)
            for i in range(2, int(last_page) + 1):
                base_url = f'http://zfcxjst.hebei.gov.cn/was5/web/search?page={str(i)}&channelid=289933&perpage=15&outlinepage=10&zsbh=&qymc=&zzlx='
                yield scrapy.Request(base_url, callback=self.parse_in_hebei)

    def parse_in_hebei_detail(self, response):
        company_name = response.xpath('//table//tr[3]/td[2]/text()').get()
        social_credit_code = response.xpath('//table//tr[4]/td[2]/text()').get()
        record_province = '河北'
        time_now = self.get_create_time()
        yield BeiAnItem(company_name=company_name, social_credit_code=social_credit_code, record_province=record_province,
                        status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        self.crawler.stats.inc_value('sw_num')

