# -*- coding: utf-8 -*-
import datetime
import re
import time

import scrapy
from urllib.parse import urlencode, urlparse, parse_qs
import copy
from ..items import BeiAnItem, AptitudeItem, CompanyItem


class FujianSpiderSpider(scrapy.Spider):
    name = 'fujian_spider'
    allowed_domains = ['220.160.52.164']
    start_urls = ['http://220.160.52.164:96/ConstructionInfoPublish/Pages/CompanyQuery.aspx']
    post_url = 'http://220.160.52.164:96/ConstructionInfoPublish/Pages/CompanyQuery.aspx'
    data = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__EVENTVALIDATION': None,
        'ctl00$ContentPlaceHolder$tbCompanyName': '',
        'ctl00$ContentPlaceHolder$ddlBussinessSystem': None,
        'ctl00$ContentPlaceHolder$btnQuery': 'Wait',
        'ctl00$ContentPlaceHolder$tbSocialCreditCode': '',
        'ctl00$ContentPlaceHolder$tbCorporationNumber': ''
    }
    data_next_page = {
        '__EVENTTARGET': 'ctl00$ContentPlaceHolder$pGrid$nextpagebtn',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__EVENTVALIDATION': None,
        'ctl00$ContentPlaceHolder$tbCompanyName': '',
        'ctl00$ContentPlaceHolder$ddlBussinessSystem': None,
        'ctl00$ContentPlaceHolder$tbSocialCreditCode': '',
        'ctl00$ContentPlaceHolder$tbCorporationNumber': ''
    }
    data_last4_page = {
        '__EVENTTARGET': None,
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        '__EVENTVALIDATION': None,
        'ctl00$ContentPlaceHolder$tbCompanyName': '',
        'ctl00$ContentPlaceHolder$ddlBussinessSystem': None,
        'ctl00$ContentPlaceHolder$tbSocialCreditCode': '',
        'ctl00$ContentPlaceHolder$tbCorporationNumber': ''
    }

    @staticmethod
    def get_province_company_id():
        return 'fujian-' + str(int(time.time()*1000))

    @staticmethod
    def get_create_time():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def parse(self, response):
        self.data['__VIEWSTATE'] = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
        self.data['__VIEWSTATEGENERATOR'] = response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').get()
        self.data['__EVENTVALIDATION'] = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()

        # 本省建筑业
        self.data['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = '39'  # 39表示省内建筑业名单
        yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse_sn_list, meta={'_type': '39'})
        # 本省招标代理
        self.data['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = '9'     # 9表示省内招标代理名单
        yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse_sn_list, meta={'_type': '9'})
        # # 省外建筑业
        self.data['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = '31'     # 31表示省外建筑业名单
        yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse_sw__list, meta={'_type': '31'})
        # # 省外建筑业
        self.data['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = '42'     # 42表示省外招标代理名单
        yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse_sw__list, meta={'_type': '42'})
        # test
        # yield scrapy.Request('http://220.160.52.164:96/ConstructionInfoPublish/Pages/CompanyQualificationInfo.aspx?companyID=13&companyguid=187AFEE44C4F43B7A8BBEA0ADECE90D1&index=0&_=1569229589469.337', callback=self.parse_sn_construction_aptitude)

    def parse_sn_list(self, response):
        hrefs = response.xpath('//table//tr//a/@href').getall()
        for href in hrefs:
            if len(href) > 30 and 'companyguid' in href:
                yield scrapy.Request(response.urljoin(href), callback=self.parse_sn_basic_detail)

        # 解析下一页，和最后4页
        current_page, total_page = response.xpath('//*[@id="ctl00_ContentPlaceHolder_pGrid"]/table//tr/td[1]').re('第(\d+)/(\d+)页')
        print(current_page, total_page)
        if int(current_page) < int(total_page) - 5:     #1554-5=1549, 小于1549可以用下一页获取， 最后4页不能用下一页获取，只能用106 107 108 109获取
            self.data_next_page['__VIEWSTATE'] = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
            self.data_next_page['__VIEWSTATEGENERATOR'] = response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').get()
            self.data_next_page['__EVENTVALIDATION'] = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()
            self.data_next_page['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = response.meta['_type']
            yield scrapy.FormRequest(self.post_url, formdata=self.data_next_page, callback=self.parse_sn_list, meta={'_type': response.meta['_type']}, priority=1)
        else:
            for i in range(6, 10):
                self.data_last4_page['__VIEWSTATE'] = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
                self.data_last4_page['__VIEWSTATEGENERATOR'] = response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').get()
                self.data_last4_page['__EVENTVALIDATION'] = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()
                self.data_last4_page['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = response.meta['_type']
                self.data_last4_page['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder$pGrid$ctl0'+str(i)
                yield scrapy.FormRequest(self.post_url, formdata=self.data_next_page, callback=self.parse_sn_list, meta={'_type': response.meta['_type']}, priority=1)

    def parse_sn_basic_detail(self, response):
        # 解析基本信息
        province_company_id = self.get_province_company_id()  # 生成此刻的企业id
        time_now = self.get_create_time()  # 生成此刻的时间戳
        social_credit_code = str(response.xpath('//td[@id="LegalManDuty"]/text()').get()).strip()  # 统一社会信用编码
        if social_credit_code == 'None' or social_credit_code == '':
            social_credit_code = str(response.xpath('//*[@id="CorpCode"]/text()').get()).strip()  # 组织机构代码
        basic_msg = {
            'province_company_id': province_company_id,
            'company_name': str(response.xpath('//*[@id="CorpName"]/text()').get()).strip(),  # 企业名称
            'leal_person': str(response.xpath('//*[@id="LegalMan"]/text()').get()).strip(),  # 法定代表人
            'regis_address': str(response.xpath('//*[@id="LicenseNum"]/text()').get()).strip(),  # 企业注册地址
            'social_credit_code': social_credit_code,
            'business_address': str(response.xpath('//*[@id="RegPrin"]/text()').get()).strip(),  # 企业详细地址
            'area_code': 350000,  # 地区码
            'source': '福建',  # 来源
            'status': 1,  # 状态
            'create_time': time_now,  # 创建时间
            'modification_time': time_now,  # 修改时间
            'is_delete': 0,  # 是否删除
            'url': response.url
        }
        yield CompanyItem(basic_msg)
        # 获取资质详细信息
        query_dict = parse_qs(urlparse(response.url).query)
        params = (
            ('companyID', query_dict.get('companyID')[0]),
            ('companyguid', query_dict.get('companyguid')[0]),
            ('index', '0'),
            ('_', str(time.time() * 1000)),
        )
        url = 'http://220.160.52.164:96/ConstructionInfoPublish/Pages/CompanyQualificationInfo.aspx?'
        yield scrapy.Request(url + urlencode(params), callback=self.parse_sn_aptitude,
                             meta=copy.deepcopy(basic_msg))

    def parse_sn_aptitude(self, response):
        trs1 = response.xpath('//tr')
        trs2 = re.findall('<tr>(.*?)</tr>', response.text, flags=re.S)
        for i, tr in enumerate(zip(trs1, trs2)):
            if i == 0:
                continue
            aptitude_id = str(tr[0].xpath('./td[1]/text()').get()).strip()  # 资质证书编号
            aptitude_type_ss = re.findall('<td>(.*?)</td>', tr[1])   # 资质类别及等级
            for aptitude_type in aptitude_type_ss:
                if '</br>' in aptitude_type:
                    aptitude_name = re.sub('</br>', '$', aptitude_type).strip('$')
                    break
            else:
                aptitude_name = None
            aptitude_organ = str(tr[0].xpath('./td[3]/text()').get()).strip()  # 发证机关
            aptitude_startime = str(tr[0].xpath('./td[4]/text()').get()).strip()  # 证书发布日期
            # aptitude_begin_date = str(tr[0].xpath('./td[5]/text()').get()).strip()  # 有效期自
            aptitude_endtime = str(tr[0].xpath('./td[6]/text()').get()).strip()  # 有效期到
            # print(aptitude_num, aptitude_type_s, aptitude_organ, aptitude_accept_date, aptitude_begin_date, aptitude_useful_date)
            yield AptitudeItem(province_company_id=response.meta['province_company_id'], company_name=response.meta['company_name'],
                               aptitude_id=aptitude_id, aptitude_name=aptitude_name,
                               aptitude_organ=aptitude_organ, aptitude_startime=aptitude_startime,
                               aptitude_endtime=aptitude_endtime,
                               source='福建', status=1, create_time=response.meta['create_time'], modification_time=response.meta['modification_time'], is_delete=0)

    def parse_sw__list(self, response):
        trs = response.xpath('//*[@id="ctl00_ContentPlaceHolder_gvDemandCompany"]//tr')
        for i, tr in enumerate(trs):
            if i == 0:
                continue
            company_name = str(tr.xpath('./td[1]/a/text()').get()).strip()  # 企业名称
            social_credit_code = str(tr.xpath('./td[6]/text()').get()).strip()    # 统一社会信用代码/组织机构代码
            time_now = self.get_create_time()   # 生成此刻的时间戳
            yield BeiAnItem(company_name=company_name, social_credit_code=social_credit_code,
                            record_province='福建',
                            status=1, create_time=time_now, modification_time=time_now, is_delete=0)
        # 解析下一页，和最后4页
        current_page, total_page = response.xpath('//*[@id="ctl00_ContentPlaceHolder_pGrid"]/table//tr/td[1]').re('第(\d+)/(\d+)页')
        print(current_page, total_page)
        if int(current_page) < int(total_page) - 5:  # 1554-5=1549, 小于1549可以用下一页获取， 最后4页不能用下一页获取，只能用106 107 108 109获取
            self.data_next_page['__VIEWSTATE'] = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
            self.data_next_page['__VIEWSTATEGENERATOR'] = response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').get()
            self.data_next_page['__EVENTVALIDATION'] = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()
            self.data_next_page['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = response.meta['_type']
            yield scrapy.FormRequest(self.post_url, formdata=self.data_next_page, callback=self.parse_sw__list,
                                     meta={'_type': response.meta['_type']})
        else:
            for i in range(6, 10):
                self.data_last4_page['__VIEWSTATE'] = response.xpath('//*[@id="__VIEWSTATE"]/@value').get()
                self.data_last4_page['__VIEWSTATEGENERATOR'] = response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').get()
                self.data_last4_page['__EVENTVALIDATION'] = response.xpath('//*[@id="__EVENTVALIDATION"]/@value').get()
                self.data_last4_page['ctl00$ContentPlaceHolder$ddlBussinessSystem'] = response.meta['_type']
                self.data_last4_page['__EVENTTARGET'] = 'ctl00$ContentPlaceHolder$pGrid$ctl0'+str(i)
                yield scrapy.FormRequest(self.post_url, formdata=self.data_next_page, callback=self.parse_sw__list,
                                         meta={'_type': response.meta['_type']})
