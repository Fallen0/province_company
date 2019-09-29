# -*- coding: utf-8 -*-
import datetime
import time

import scrapy
import re

from scrapy import signals

from ..items import CompanyItem, BeiAnItem, AptitudeItem
import traceback


class Shanxi1SpiderSpider(scrapy.Spider):
    name = 'shanxi1_spider'
    allowed_domains = ['zjt.shanxi.gov.cn']

    def _set_crawler(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        crawler.stats.set_value("sn_num", 0)
        crawler.stats.set_value("sw_num", 0)
        crawler.signals.connect(self.close, signals.spider_closed)

    @staticmethod
    def get_province_company_id():
        return 'shan1xi-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def start_requests(self):
        # 建筑业
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/JzyList.aspx', callback=self.parse, meta={'callback': self.parse_construction_detail})
        # 安全许可证
        # yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/AqscList.aspx', callback=self.parse, meta={'callback': self.parse_security_detail})
        # # 工程监理
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/GcjlList.aspx', callback=self.parse, meta={'callback': self.parse_supervisor_detail})
        # # 工程勘察
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/GckcList.aspx', callback=self.parse, meta={'callback': self.parse_reconnaissance_detail})
        # # 工程设计
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/Ent_DesignList.aspx', callback=self.parse, meta={'callback': self.parse_design_detail})
        # # 工程造价
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/GczjList.aspx', callback=self.parse, meta={'callback': self.parse_cost_detail})
        # # 设计与施工
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/Ent_DesignBuildList.aspx', callback=self.parse, meta={'callback': self.parse_design_construction_detail})
        # # 城乡规划
        # yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/EntQua_Cxgh.aspx', callback=self.parse, meta={'callback': self.parse_town_country_detail})
        # 外籍备案
        yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/EnterJinList.aspx', callback=self.parse_record_detail, meta={'callback': self.parse_record_detail})
        # error_test_detail
        # yield scrapy.Request('http://zjt.shanxi.gov.cn/jzscNew/Browse/JzyList_1.aspx?ent_id=17299&cert_id=13c34c26-4b75-4706-9016-5738239786e3', callback=self.parse_construction_detail)

    def parse_last_page(self, response):
        base_url = response.url + '?page='
        if 'page' not in response.url:
            last_page_href = response.xpath('//div[@class="page"]/a[contains(text(), "尾页")]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            print(last_page, response.meta)
            for i in range(2, int(last_page) + 1):
                aim_url = base_url + str(i)
                yield scrapy.Request(aim_url, callback=self.parse, meta={'callback': response.meta['callback']})

    def parse(self, response):
        # 解析table获得详情页url
        trs = response.xpath('//form[@id="formList"]/table//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            company_url = tr.xpath('./td[1]/a/@onclick').get()  # 企业详细页url
            if company_url:
                path_params = re.search("'(.*?)'", company_url).group(1)
                yield scrapy.Request(response.urljoin(path_params), callback=response.meta['callback'])
        for j in self.parse_last_page(response):      # 是第一页，就去找最后一页
            yield j

    def parse_construction_detail_special(self, response, tables):
        for i, table in enumerate(tables):
            if i == 0:
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                registered_capital = table.xpath('.//tr[4]/td[2]/text()').get().strip()  # 注册资本金
                if registered_capital and bool(re.search('(\d)', registered_capital)):
                    registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                else:
                    registered_capital = None
                build_date = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 企业成立日期
                social_credit_code = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 营业执照注册号
                regis_type = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 经济性质
                aptitude_organ = str(table.xpath('.//tr[6]/td[2]//text()').get()).strip()  # 发证机关/管理部门
                aptitude_startime = str(table.xpath('.//tr[6]/td[4]//text()').get()).strip()  # 发证日期
                aptitude_id = str(table.xpath('.//tr[7]/td[2]/text()').get()).strip()  # 证书编号
                leal_person = str(table.xpath('.//tr[10]/td[2]/text()').get()).strip()  # 法定代表人
                leal_person_duty = str(table.xpath('.//tr[10]/td[3]/text()').get()).strip()  # 法定代表人职务
                leal_person_title = str(table.xpath('.//tr[10]/td[4]/text()').get()).strip()  # 法定代表人职称

                ceoname = str(table.xpath('.//tr[11]/td[2]/text()').get()).strip()  # 企业负责人
                # contact_person_duty = str(table.xpath('.//tr[11]/td[3]/text()').get()).strip()  # 企业负责人职务

                ctoname = str(table.xpath('.//tr[12]/td[2]/text()').get()).strip()  # 技术负责人
                # tech_lead_title = str(table.xpath('.//tr[12]/td[4]/text()').get()).strip()  # 企业负责人职务
                yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                  business_address=business_address, registered_capital=registered_capital,
                                  build_date=build_date, social_credit_code=social_credit_code,
                                  regis_type=regis_type, leal_person=leal_person,
                                  leal_person_duty=leal_person_duty, leal_person_title=leal_person_title,
                                  ceoname=ceoname, ctoname=ctoname,
                                  url=response.url, danweitype='建筑业',
                                  area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            else:
                # 资质信息(可能多条,可能没有)
                trs = table.xpath('.//tr')
                if len(trs) > 2:
                    for j, tr in enumerate(trs):
                        if j < 2:
                            continue
                        aptitude_large = str(tr.xpath('./td[2]/text()').get()).strip()  # 资质序列，实际是资质大类
                        aptitude_name = str(tr.xpath('./td[3]/text()').get()).strip()   # 专业及等级/资质name
                        check_time = str(tr.xpath('./td[4]/text()').get()).strip()   # 核准日期
                        yield AptitudeItem(province_company_id=province_company_id, aptitude_organ=aptitude_organ,
                                           aptitude_startime=aptitude_startime, aptitude_id=aptitude_id,
                                           aptitude_large=aptitude_large, aptitude_name=aptitude_name,
                                           check_time=check_time, company_name=company_name,
                                           source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_construction_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        if '负责人信息' in tables.get():
            for j in self.parse_construction_detail_special(response, tables):
                yield j
        else:
            for table in tables:
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                social_credit_code = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 营业执照注册号
                leal_person = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 法定代表人
                registered_capital = table.xpath('.//tr[5]/td[2]/text()').get().strip()  # 注册资本
                if registered_capital and bool(re.search('(\d)', registered_capital)):
                    registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                else:
                    registered_capital = None
                regis_type = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 经济性质
                aptitude_id = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 证书编号
                aptitude_endtime = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 证书有效期止
                aptitude_name = str(table.xpath('.//tr[7]/td[2]//text()').get()).strip()  # 资质范围
                aptitude_organ = str(table.xpath('.//tr[8]/td[2]//text()').get()).strip()  # 发证机关
                aptitude_startime = str(table.xpath('.//tr[8]/td[4]//text()').get()).strip()  # 发证日期
                yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                  business_address=business_address, social_credit_code=social_credit_code,
                                  leal_person=leal_person, registered_capital=registered_capital,
                                  regis_type=regis_type,
                                  url=response.url, danweitype='建筑业',
                                  area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
                yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                   aptitude_endtime=aptitude_endtime, aptitude_name=aptitude_name,
                                   aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                                   company_name=company_name,
                                   source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_security_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        for table in tables:
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(table.xpath('.//tr[1]/td[2]/text()').get()).strip()  # 单位名称
            business_address = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 单位地址
            aptitude_id = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 证书编号
            ceoname = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 主要负责人
            regis_type = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 经济性质/类型
            aptitude_name = str(table.xpath('.//tr[5]/td[2]//text()').get()).strip()  # 资质范围
            aptitude_endtime = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 证书有效期止
            if aptitude_endtime and '到' in aptitude_endtime:
                aptitude_endtime = aptitude_endtime.split('到')[-1].strip()
            aptitude_organ = str(table.xpath('.//tr[7]/td[2]//text()').get()).strip()  # 发证机关
            aptitude_startime = str(table.xpath('.//tr[7]/td[4]//text()').get()).strip()  # 发证日期
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              business_address=business_address, ceoname=ceoname,
                              regis_type=regis_type,
                              url=response.url,
                              area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                               aptitude_name=aptitude_name, aptitude_endtime=aptitude_endtime,
                               aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                               company_name=company_name,
                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_supervisor_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        if len(tables) % 2 != 0:
            raise IndexError('工程监理出现不是偶数条数据情况')
        for i, table in enumerate(tables):
            if i % 2 == 0:
                # 基础信息
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                build_date = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 建立时间
                registered_capital = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 注册资本
                if registered_capital and bool(re.search('(\d)', registered_capital)):
                    registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                else:
                    registered_capital = None
                social_credit_code = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 营业执照号
                regis_type = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 经济性质/类型
                aptitude_id = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 证书编号
                aptitude_endtime = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 证书有效期止
                aptitude_organ = str(table.xpath('.//tr[7]/td[2]//text()').get()).strip()  # 发证机关
                aptitude_startime = str(table.xpath('.//tr[7]/td[4]//text()').get()).strip()  # 发证日期

                leal_person = str(table.xpath('.//tr[8]/td[2]/text()').get()).strip()  # 法定代表人
                leal_person_duty = str(table.xpath('.//tr[8]/td[4]/text()').get()).strip()  # 法定代表人职务
                ceoname = str(table.xpath('.//tr[9]/td[2]/text()').get()).strip()  # 企业负责人
                # contact_person_duty = str(table.xpath('.//tr[9]/td[4]/text()').get()).strip()  # 企业负责人职务/联系人职务
                ctoname = str(table.xpath('.//tr[10]/td[2]/text()').get()).strip()  # 技术负责人
                # tech_lead_title = str(table.xpath('.//tr[10]/td[4]/text()').get()).strip()  # 技术负责人职称
                yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                  business_address=business_address, build_date=build_date,
                                  registered_capital=registered_capital, social_credit_code=social_credit_code,
                                  regis_type=regis_type, leal_person=leal_person,
                                  leal_person_duty=leal_person_duty, ceoname=ceoname,
                                  ctoname=ctoname, url=response.url,
                                  area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            else:
                # 资质信息(可能多条,可能没有)
                trs = table.xpath('.//tr')
                if len(trs) > 2:
                    for j, tr in enumerate(trs):
                        if j < 2:
                            continue
                        aptitude_large = str(tr.xpath('./td[1]/text()').get()).strip()  # 资质序列
                        if aptitude_large != '' and aptitude_large != 'None' and '工程监理' not in aptitude_large:
                            aptitude_large = '工程监理' + aptitude_large
                        aptitude_name = str(tr.xpath('./td[2]/text()').get()).strip()  # 专业及等级/资质name
                        check_time = str(tr.xpath('./td[3]/text()').get()).strip()  # 核准日期
                        yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                           aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                                           aptitude_startime=aptitude_startime, aptitude_large=aptitude_large,
                                           aptitude_name=aptitude_name, check_time=check_time,
                                           company_name=company_name,
                                           source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_reconnaissance_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        if len(tables) % 2 != 0:
            raise IndexError('工程勘察出现不是偶数条数据情况')
        odd_sign = 0    # 偶数标识符
        for i, table in enumerate(tables):
            # 有主管部门类型的表结构
            if odd_sign % 2 == 1 or '主管部门' in table.get():
                odd_sign = 1        # 表示偶数条资质信息走这个if
                if i % 2 == 0:
                    # 基础信息
                    province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                    time_now = self.get_create_time()  # 生成此刻的时间戳
                    company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                    business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                    department = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 主管部门
                    build_date = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 建立时间
                    regis_type = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 经济性质/类型
                    registered_capital = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 注册资本
                    if registered_capital and bool(re.search('(\d)', registered_capital)):
                        registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                    else:
                        registered_capital = None
                    aptitude_id = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 证书编号
                    # level = str(table.xpath('.//tr[7]/td[2]/text()').get()).strip()  # 证书等级
                    aptitude_organ = str(table.xpath('.//tr[8]/td[2]//text()').get()).strip()  # 发证机关
                    aptitude_startime = str(table.xpath('.//tr[8]/td[4]//text()').get()).strip()  # 发证日期
                    aptitude_endtime = str(table.xpath('.//tr[9]/td[2]/text()').get()).strip()  # 证书有效期止
                    if aptitude_endtime and aptitude_endtime != 'None':
                        aptitude_endtime = '-'.join(re.findall('(\d+)', aptitude_endtime))  # 格式化证书有效期:2019-01-01
                    leal_person = str(table.xpath('.//tr[10]/td[2]/text()').get()).strip()  # 法定代表人
                    leal_person_duty = str(table.xpath('.//tr[10]/td[4]/text()').get()).strip()  # 法定代表人职务
                    ceoname = str(table.xpath('.//tr[11]/td[2]/text()').get()).strip()  # 企业负责人
                    # contact_person_duty = str(table.xpath('.//tr[11]/td[4]/text()').get()).strip()  # 企业负责人职务
                    yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                      business_address=business_address, department=department,
                                      build_date=build_date, regis_type=regis_type,
                                      registered_capital=registered_capital, leal_person=leal_person,
                                      leal_person_duty=leal_person_duty, ceoname=ceoname, url=response.url,
                                      area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
                else:
                    odd_sign = 0  # 复原odd_sign
                    # 资质信息(可能多条,可能没有)
                    trs = table.xpath('.//tr')
                    if len(trs) > 2:
                        for j, tr in enumerate(trs):
                            if j < 2:
                                continue
                            aptitude_large = str(tr.xpath('./td[1]/text()').get()).strip()  # 资质序列
                            if aptitude_large != '' and aptitude_large != 'None' and '工程勘察' not in aptitude_large:
                                aptitude_large = '工程勘察' + aptitude_large
                            aptitude_name = str(tr.xpath('./td[2]/text()').get()).strip()  # 专业及等级/资质name
                            check_time = str(tr.xpath('./td[3]/text()').get()).strip()  # 核准时间
                            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                               aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                                               aptitude_endtime=aptitude_endtime, aptitude_large=aptitude_large,
                                               aptitude_name=aptitude_name, check_time=check_time,
                                               company_name=company_name,
                                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            # 没有主管部门类型的表结构
            else:
                odd_sign = 0  # 表示偶数条资质信息走这个if, 这里不需要复原odd_sign, 因为已经复原了
                if i % 2 == 0:
                    # 基础信息
                    province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                    time_now = self.get_create_time()  # 生成此刻的时间戳
                    company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                    business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                    build_date = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 建立时间
                    regis_type = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 经济性质/类型
                    social_credit_code = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 营业执照号
                    registered_capital = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 注册资本
                    if registered_capital and bool(re.search('(\d)', registered_capital)):
                        registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                    else:
                        registered_capital = None
                    aptitude_id = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 证书编号
                    aptitude_organ = str(table.xpath('.//tr[7]/td[2]//text()').get()).strip()  # 发证机关
                    aptitude_startime = str(table.xpath('.//tr[7]/td[4]//text()').get()).strip()  # 发证日期
                    aptitude_endtime = str(table.xpath('.//tr[8]/td[2]/text()').get()).strip()  # 证书有效期止
                    if aptitude_endtime and aptitude_endtime != 'None':
                        aptitude_endtime = '-'.join(re.findall('(\d+)', aptitude_endtime))  # 格式化证书有效期:2019-01-01
                    leal_person = str(table.xpath('.//tr[9]/td[2]/text()').get()).strip()  # 法定代表人
                    leal_person_duty = str(table.xpath('.//tr[9]/td[4]/text()').get()).strip()  # 法定代表人职务
                    ceoname = str(table.xpath('.//tr[10]/td[2]/text()').get()).strip()  # 企业负责人
                    # contact_person_duty = str(table.xpath('.//tr[10]/td[4]/text()').get()).strip()  # 企业负责人职务/联系人职务
                    ctoname = str(table.xpath('.//tr[11]/td[2]/text()').get()).strip()  # 技术负责人
                    # tech_lead_title = str(table.xpath('.//tr[11]/td[4]/text()').get()).strip()  # 技术负责人职称
                    yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                      business_address=business_address, ctoname=ctoname, social_credit_code=social_credit_code,
                                      build_date=build_date, regis_type=regis_type,
                                      registered_capital=registered_capital, leal_person=leal_person,
                                      leal_person_duty=leal_person_duty, ceoname=ceoname, url=response.url,
                                      area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
                else:
                    # 资质信息(可能多条,可能没有)
                    trs = table.xpath('.//tr')
                    if len(trs) > 2:
                        for j, tr in enumerate(trs):
                            if j < 2:
                                continue
                            aptitude_large = str(tr.xpath('./td[1]/text()').get()).strip()  # 资质序列
                            if aptitude_large != '' and aptitude_large != 'None' and '工程勘察' not in aptitude_large:
                                aptitude_large = '工程勘察' + aptitude_large
                            aptitude_name = str(tr.xpath('./td[2]/text()').get()).strip()  # 专业及等级/资质name
                            check_time = str(tr.xpath('./td[3]/text()').get()).strip()  # 核准时间
                            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                               aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                                               aptitude_endtime=aptitude_endtime, aptitude_large=aptitude_large,
                                               aptitude_name=aptitude_name, check_time=check_time,
                                               company_name=company_name,
                                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_design_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        if len(tables) % 2 != 0:
            raise IndexError('工程设计出现不是偶数条数据情况')
        for i, table in enumerate(tables):
            if i % 2 == 0:
                # 基础信息
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
                business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
                build_date = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 建立时间
                registered_capital = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 注册资本
                if registered_capital and bool(re.search('(\d)', registered_capital)):
                    registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
                else:
                    registered_capital = None
                social_credit_code = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 营业执照号
                regis_type = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 经济性质/类型
                leal_person = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 法定代表人
                leal_person_duty = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 法定代表人职务
                ceoname = str(table.xpath('.//tr[7]/td[2]/text()').get()).strip()  # 企业负责人
                # contact_person_duty = str(table.xpath('.//tr[7]/td[4]/text()').get()).strip()  # 企业负责人职务/联系人职务
                ctoname = str(table.xpath('.//tr[8]/td[2]/text()').get()).strip()  # 技术负责人
                # tech_lead_title = str(table.xpath('.//tr[8]/td[4]/text()').get()).strip()  # 技术负责人职称
                aptitude_id = str(table.xpath('.//tr[9]/td[2]/text()').get()).strip()  # 证书编号
                aptitude_endtime = str(table.xpath('.//tr[9]/td[4]/text()').get()).strip()  # 证书有效期止
                aptitude_organ = str(table.xpath('.//tr[10]/td[2]//text()').get()).strip()  # 发证机关
                aptitude_startime = str(table.xpath('.//tr[10]/td[4]//text()').get()).strip()  # 发证日期
                yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                                  business_address=business_address, build_date=build_date,
                                  registered_capital=registered_capital, social_credit_code=social_credit_code,
                                  regis_type=regis_type, leal_person=leal_person,
                                  leal_person_duty=leal_person_duty, ceoname=ceoname,
                                  ctoname=ctoname, url=response.url,
                                  area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            else:
                # 资质信息(可能多条,可能没有)
                trs = table.xpath('.//tr')
                if len(trs) > 2:
                    for j, tr in enumerate(trs):
                        if j < 2:
                            continue
                        aptitude_large = str(tr.xpath('./td[1]/text()').get()).strip()  # 资质序列/资质大类
                        if aptitude_large != '' and aptitude_large != 'None' and '工程设计' not in aptitude_large:
                            aptitude_large = '工程设计' + aptitude_large
                        aptitude_small = str(tr.xpath('./td[2]/text()').get()).strip()  # 资质小类
                        level = str(tr.xpath('./td[3]/text()').get()).strip()  # 专业等级
                        check_time = str(tr.xpath('./td[4]/text()').get()).strip()  # 核准时间
                        yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                                           aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                                           aptitude_startime=aptitude_startime, aptitude_large=aptitude_large,
                                           aptitude_small=aptitude_small, level=level,
                                           check_time=check_time, company_name=company_name,
                                           source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_cost_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        for i, table in enumerate(tables):
            # 基础信息
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
            business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
            social_credit_code = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 营业执照号
            regis_type = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 经济性质/类型
            registered_capital = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 注册资本
            if registered_capital and bool(re.search('(\d)', registered_capital)):
                registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
            else:
                registered_capital = None
            build_date = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 建立时间
            leal_person = str(table.xpath('.//tr[7]/td[2]/text()').get()).strip()  # 法定代表人
            leal_person_title = str(table.xpath('.//tr[7]/td[4]/text()').get()).strip()  # 法定代表人职称
            ctoname = str(table.xpath('.//tr[8]/td[2]/text()').get()).strip()  # 技术负责人
            # tech_lead_title = str(table.xpath('.//tr[8]/td[4]/text()').get()).strip()  # 技术负责人职称
            aptitude_id = str(table.xpath('.//tr[10]/td[2]/text()').get()).strip()  # 证书编号
            aptitude_endtime = str(table.xpath('.//tr[11]/td[4]/text()').get()).strip()  # 证书有效期止
            aptitude_organ = str(table.xpath('.//tr[12]/td[2]//text()').get()).strip()  # 发证机关
            aptitude_startime = str(table.xpath('.//tr[12]/td[4]//text()').get()).strip()  # 发证日期
            aptitude_name = str(table.xpath('.//tr[13]/td[2]/text()').get()).strip()  # 业务范围
            if aptitude_name:
                x = list(map(lambda x: re.sub("\s", "", x), aptitude_name))
                aptitude_name = ''.join(x)                                # 去掉业务范围中间的空格
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              business_address=business_address, social_credit_code=social_credit_code,
                              regis_type=regis_type, registered_capital=registered_capital,
                              build_date=build_date, leal_person=leal_person,
                              leal_person_title=leal_person_title, ctoname=ctoname,
                              url=response.url,
                              area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                               aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                               aptitude_startime=aptitude_startime, aptitude_name=aptitude_name,
                               company_name=company_name,
                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_design_construction_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        for i, table in enumerate(tables):
            # 基础信息
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业名称
            business_address = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 单位地址
            registered_capital = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 注册资本
            if registered_capital and bool(re.search('(\d)', registered_capital)):
                registered_capital = re.search('([\d|\.]+)', registered_capital).group(1)  # 格式化注册资本，只有数字
            else:
                registered_capital = None
            leal_person = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 法定代表人
            ceoname = str(table.xpath('.//tr[6]/td[2]/text()').get()).strip()  # 单位负责人
            aptitude_id = str(table.xpath('.//tr[7]/td[2]/text()').get()).strip()  # 证书编号
            aptitude_endtime = str(table.xpath('.//tr[7]/td[4]/text()').get()).strip()  # 证书有效期止
            aptitude_organ = str(table.xpath('.//tr[8]/td[2]//text()').get()).strip()  # 发证机关
            aptitude_startime = str(table.xpath('.//tr[8]/td[4]//text()').get()).strip()  # 发证日期
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              business_address=business_address, registered_capital=registered_capital,
                              leal_person=leal_person, ceoname=ceoname,
                              url=response.url,
                              area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            yield AptitudeItem(province_company_id=province_company_id, aptitude_id=aptitude_id,
                               aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                               aptitude_startime=aptitude_startime, company_name=company_name,
                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_town_country_detail(self, response):
        tables = response.xpath('//ul[contains(@style,"block")]//table')
        for table in tables:
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(table.xpath('.//tr[1]/td[2]/text()').get()).strip()  # 企业名称
            business_address = str(table.xpath('.//tr[2]/td[2]/text()').get()).strip()  # 企业地址
            leal_person = str(table.xpath('.//tr[3]/td[2]/text()').get()).strip()  # 法定代表人
            tel = str(table.xpath('.//tr[4]/td[2]/text()').get()).strip()  # 联系电话
            fax = str(table.xpath('.//tr[4]/td[4]/text()').get()).strip()  # 传真
            aptitude_id = str(table.xpath('.//tr[5]/td[2]/text()').get()).strip()  # 证书编号
            level = str(table.xpath('.//tr[5]/td[4]/text()').get()).strip()  # 证书等级
            aptitude_endtime = str(table.xpath('.//tr[6]/td[4]/text()').get()).strip()  # 证书有效期止
            aptitude_organ = str(table.xpath('.//tr[7]/td[2]//text()').get()).strip()  # 发证机关/管理部门
            aptitude_startime = str(table.xpath('.//tr[7]/td[4]//text()').get()).strip()  # 发证日期
            aptitude_name = str(table.xpath('.//tr[8]/td[2]//text()').get()).strip()  # 资质范围
            if aptitude_name:
                x = list(map(lambda x: re.sub("\s", "", x), aptitude_name))
                aptitude_name = ''.join(x)                                # 去掉资质范围中间的空格

            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              business_address=business_address, leal_person=leal_person,
                              tel=tel, fax=fax, url=response.url,
                              area_code=140000, source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            yield AptitudeItem(aptitude_id=aptitude_id, level=level, province_company_id=province_company_id,
                               aptitude_endtime=aptitude_endtime, aptitude_organ=aptitude_organ,
                               aptitude_startime=aptitude_startime, aptitude_name=aptitude_name,
                               company_name=company_name,
                               source='山西', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_last_page_beian(self, response):
        base_url = response.url + '?page='
        if 'page' not in response.url:
            last_page_href = response.xpath('//div[@class="page"]/a[contains(text(), "尾页")]/@href').get()
            last_page = re.search('page=(\d+)', last_page_href).group(1)
            for i in range(2, int(last_page) + 1):
                aim_url = base_url + str(i)
                yield scrapy.Request(aim_url, callback=self.parse_record_detail)

    def parse_record_detail(self, response):
        # 解析table获得详情页url
        trs = response.xpath('//form[@id="formList"]/table//tr')
        for i, tr in enumerate(trs):
            if i == 0 or i >= 15:
                continue
            try:
                time_now = self.get_create_time()  # 生成此刻的时间戳
                company_name = str(tr.xpath('./td[1]/a/text()').get()).strip()  # 企业名称
                # danweitype = tr.xpath('./td[2]/text()').get().strip()  # 企业类型
                social_credit_code = str(tr.xpath('./td[3]/text()').get()).strip()  # 组织机构编码
                # areaname = tr.xpath('./td[4]/text()').get().strip()  # 注册所在地辖区名称
                yield BeiAnItem(company_name=company_name, social_credit_code=social_credit_code, record_province='山西',
                                status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            except:
                with open('error.txt', 'a') as f:
                    f.write(f"{response.url}--{traceback.format_exc()}\n")
                    f.flush()

        for j in self.parse_last_page_beian(response):      # 是第一页，就去找最后一页
            yield j


