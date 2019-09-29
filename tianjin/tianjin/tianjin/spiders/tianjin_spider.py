# -*- coding: utf-8 -*-
import datetime
import time

import scrapy
import re

from scrapy import signals

from ..items import CompanyItem, AptitudeItem, BeiAnItem


class TianjinSpiderSpider(scrapy.Spider):
    handle_httpstatus_list = [403]      # 这里的403表示服务器崩溃了，不是forbidden
    name = 'tianjin_spider'
    allowed_domains = ['218.69.33.156']
    start_urls = ['http://218.69.33.156/epr/Search/C_Common/CE/index.aspx']
    post_url = 'http://218.69.33.156/epr/Search/C_Common/CE/index.aspx'
    # 资质map
    mark_map = {2: '建筑业企业资质', 3: '工程监理企业资质', 4: '造价咨询企业资质', 5: '招标代理企业资质', 6: '项目代建企业资质',
                7: '园林企业资质', 8: '勘查企业资质', 9: '设计企业资质', 10: '图审机构企业资质', 11: '检测机构企业资质'}
    #
    data = {'__EVENTTARGET': '',  # 'ASPxGridView2$cell0_6$LinkButton1'
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': None,
            '__VIEWSTATEGENERATOR': None,
            '__EVENTVALIDATION': None,
            'qymc': '',
            'qyfl_VI': '本市',
            'qyfl': '本市',
            'qyfl_DDDWS': '0:0:-1:0:0:0:0:',
            'qyfl_DDD_LDeletedItems': '',
            'qyfl_DDD_LInsertedItems': '',
            'qyfl_DDD_L_VI': '本市',
            'gcszq_VI': '',
            'gcszq': '',
            'gcszq_DDDWS': '0:0:-1:0:0:0:0:',
            'gcszq_DDD_LDeletedItems': '',
            'gcszq_DDD_LInsertedItems': '',
            'gcszq_DDD_L_VI': '',
            'qylb_VI': '',
            'qylb': '',
            'qylb_DDDWS': '0:0:-1:0:0:0:0:',
            'qylb_DDD_LDeletedItems': '',
            'qylb_DDD_LInsertedItems': '',
            'qylb_DDD_L_VI': '',
            'ASPxGridView2$DXSelInput': '',
            'ASPxGridView2$CallbackState': None
            }
    #
    data2 = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        'qymc': '',
        'qyfl_VI': '本市',
        'qyfl': '本市',
        'qyfl_DDDWS': '0:0:-1:0:0:0:0:',
        'qyfl_DDD_LDeletedItems': '',
        'qyfl_DDD_LInsertedItems': '',
        'qyfl_DDD_L_VI': '本市',
        'gcszq_VI': '',
        'gcszq': '',
        'gcszq_DDDWS': '0:0:-1:0:0:0:0:',
        'gcszq_DDD_LDeletedItems': '',
        'gcszq_DDD_LInsertedItems': '',
        'gcszq_DDD_L_VI': '',
        'qylb_VI': '',
        'qylb': '',
        'qylb_DDDWS': '0:0:-1:0:0:0:0:',
        'qylb_DDD_LDeletedItems': '',
        'qylb_DDD_LInsertedItems': '',
        'qylb_DDD_L_VI': '',
        'ASPxGridView2$DXSelInput': '',
        'ASPxGridView2$CallbackState': None,
        '__CALLBACKID': 'ASPxGridView2',
        '__CALLBACKPARAM': 'GB|16;PAGERONCLICK|PBN;',
        '__EVENTVALIDATION': None
    }
    #
    data_sw = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': None,
        '__VIEWSTATEGENERATOR': None,
        'qymc': '',
        'qyfl_VI': '外地',
        'qyfl': '外地',
        'qyfl_DDDWS': '0:0:-1:0:0:0:0:',
        'qyfl_DDD_LDeletedItems': '',
        'qyfl_DDD_LInsertedItems': '',
        'qyfl_DDD_L_VI': '外地',
        'gcszq_VI': '',
        'gcszq': '',
        'gcszq_DDDWS': '0:0:-1:0:0:0:0:',
        'gcszq_DDD_LDeletedItems': '',
        'gcszq_DDD_LInsertedItems': '',
        'gcszq_DDD_L_VI': '',
        'qylb_VI': '',
        'qylb': '',
        'qylb_DDDWS': '0:0:-1:0:0:0:0:',
        'qylb_DDD_LDeletedItems': '',
        'qylb_DDD_LInsertedItems': '',
        'qylb_DDD_L_VI': '',
        'ASPxGridView2$DXSelInput': '',
        'ASPxGridView2$CallbackState': None,
        '__CALLBACKID': 'ASPxGridView2',
        '__CALLBACKPARAM': 'GB|16;PAGERONCLICK|PBN;',
        '__EVENTVALIDATION': None
    }

    @staticmethod
    def get_province_company_id():
        return 'tianjin-' + str(int(time.time()*1000))

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
        form1 = response.xpath('//form[@id="form1"]')
        __VIEWSTATE = form1.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
        __VIEWSTATEGENERATOR = form1.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
        __EVENTVALIDATION = form1.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
        ASPxGridView2_CallbackState = form1.xpath('.//input[@name="ASPxGridView2$CallbackState"]/@value').get()
        data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': __VIEWSTATE,
            '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': __EVENTVALIDATION,
            'qymc': '',
            'qyfl_VI': '本市',
            'qyfl': '本市',
            'qyfl_DDDWS': '0:0:10002:436:125:72:104',  # 浏览器特征检测，先写死
            'qyfl_DDD_LDeletedItems': '',
            'qyfl_DDD_LInsertedItems': '',
            'qyfl_DDD_L_VI': '本市',
            'gcszq_VI': '',
            'gcszq': '',
            'gcszq_DDDWS': '0:0:-1:0:0:0:0:',
            'gcszq_DDD_LDeletedItems': '',
            'gcszq_DDD_LInsertedItems': '',
            'gcszq_DDD_L_VI': '',
            'qylb_VI': '',
            'qylb': '',
            'qylb_DDDWS': '0:0:-1:0:0:0:0:',
            'qylb_DDD_LDeletedItems': '',
            'qylb_DDD_LInsertedItems': '',
            'qylb_DDD_L_VI': '',
            'Button1': '查询',
            'ASPxGridView2$DXSelInput': '',
            'ASPxGridView2$CallbackState': ASPxGridView2_CallbackState
        }
        yield scrapy.FormRequest(self.post_url, formdata=data, callback=self.parse_sn)
        data['qyfl_VI'] = '外地'
        data['qyfl'] = '外地'
        data['qyfl_DDD_L_VI'] = '外地'
        yield scrapy.FormRequest(self.post_url, formdata=data, callback=self.parse_sw)
        # error_test
        # yield scrapy.Request('http://218.69.33.156/epr/Search/CE/CKE.aspx?k_qyzcjlh=cd3a2040-b63a-46b7-9ec3-c98f05e63019', callback=self.parse_sn_detail)

    def parse_sn(self, response):
        if response.status == 403:
            time.sleep(60)
            response.request.dont_filter = True
            yield response.request
        else:
            form1 = response.xpath('//form[@id="form1"]')
            if form1.get():
                # 第一页时全部都有
                __VIEWSTATE = form1.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
                __VIEWSTATEGENERATOR = form1.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
                __EVENTVALIDATION = form1.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
                ASPxGridView2_CallbackState = form1.xpath('.//input[@name="ASPxGridView2$CallbackState"]/@value').get()
                self.data['__VIEWSTATE'] = __VIEWSTATE
                self.data['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
                self.data['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState

                self.data2['__VIEWSTATE'] = __VIEWSTATE
                self.data2['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
                self.data2['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data2['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState
            else:
                # 下一页时，只会变化__EVENTVALIDATION和ASPxGridView2_CallbackState
                __EVENTVALIDATION = re.search('\d+\|(.*?)< ?table', response.text).group(1)
                ASPxGridView2_CallbackState = response.xpath('.//input[@name="ASPxGridView2$CallbackState"]/@value').get()
                self.data['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data2['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState
                self.data2['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState
            # 获取每页的10个url
            hrefs = response.xpath('//table[@id="ASPxGridView2_DXMainTable"]//tr/td[7]/a/@href').getall()
            for href in hrefs:
                __EVENTTARGET = re.search("doPostBack\('(.*?)'", href).group(1)
                self.data['__EVENTTARGET'] = __EVENTTARGET
                yield scrapy.FormRequest(self.post_url, formdata=self.data, callback=self.parse_sn_location)

            # 解析如果有下一页，则获取下一页
            onclick_attributes = response.xpath('//td[@class="dxpControl"]//td[@class="dxpButton"]/@onclick').getall()
            for onclick_attribute in onclick_attributes:
                if 'PBN' in onclick_attribute:
                    yield scrapy.FormRequest(self.post_url, formdata=self.data2, callback=self.parse_sn, priority=1)
                    break

    def parse_sn_location(self, response):
        if response.status == 403:
            time.sleep(60)
            response.request.dont_filter = True
            yield response.request
        else:
            # 获取详细页url
            location = re.search("window.open\('(.*?)'\)", response.text).group(1)
            detail_url = response.urljoin(location)
            yield scrapy.Request(detail_url, callback=self.parse_sn_detail)

    def parse_sn_detail(self, response):
        if response.status == 403:
            time.sleep(60)
            response.request.dont_filter = True
            yield response.request
        else:
            # 基础信息
            province_company_id = self.get_province_company_id()  # 生成此刻的企业id
            time_now = self.get_create_time()  # 生成此刻的时间戳
            company_name = str(response.xpath('//*[@id="FormView1_qymcLabel"]/text()').get()).strip()  # 企业名称
            regis_type = str(response.xpath('//*[@id="FormView1_jjlxLabel"]/text()').get()).strip()  # 经济性质
            registered_capital = str(response.xpath('//*[@id="FormView1_zczjLabel"]/text()').get()).strip()  # 注册资本
            build_date = str(response.xpath('//*[@id="FormView1_clsjLabel"]/text()').get()).strip()  # 成立时间
            leal_person = str(response.xpath('//*[@id="FormView1_qyfrLabel"]/text()').get()).strip()  # 法定代表人
            yield CompanyItem(province_company_id=province_company_id, company_name=company_name,
                              regis_type=regis_type, registered_capital=registered_capital,
                              build_date=build_date, leal_person=leal_person,
                              url=response.url,
                              area_code=120000, source='天津', status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sn_num')
            print('省内：', self.crawler.stats.get_value('sn_num'))
            # 资质信息
            for i in range(2, 12):
                aptitude_id = response.xpath(f'//form[@name="form1"]//table[2]//tr[{str(i)}]/td[2]//text()').getall()  # 资质证号
                aptitude_id = ' '.join(aptitude_id).strip()
                if i == 2:
                    aptitude_name = response.xpath(f'//form[@name="form1"]//table[2]//tr[{str(i)}]/td[4]//text()').getall()  # 资质分类
                else:
                    aptitude_name = response.xpath(f'//form[@name="form1"]//table[2]//tr[{str(i)}]/td[3]//text()').getall()  # 资质分类
                aptitude_name = '$'.join(aptitude_name).strip()
                aptitude_name = list(map(lambda x: re.sub('''[\s|'|"|,]''', '', x), aptitude_name))
                aptitude_name = ''.join(aptitude_name).strip('$')
                if aptitude_id or aptitude_name:
                    yield AptitudeItem(province_company_id=province_company_id, company_name=company_name, aptitude_id=aptitude_id,
                                       aptitude_name=aptitude_name, aptitude_type=self.mark_map[i],
                                       source='天津', status=1, create_time=time_now, modification_time=time_now, is_delete=0)

    def parse_sw(self, response):
        if response.status == 403:
            time.sleep(60)
            response.request.dont_filter = True
            yield response.request
        else:
            corpnames = response.xpath('//table[@id="ASPxGridView2_DXMainTable"]//tr/td[2]/text()').getall()
            for company_name in corpnames:
                company_name = company_name.strip()
                if company_name:
                    time_now = self.get_create_time()  # 生成此刻的时间戳
                    yield BeiAnItem(company_name=company_name, record_province='天津',
                                    status=1, create_time=time_now, modification_time=time_now, is_delete=0)
                    self.crawler.stats.inc_value('sw_num')
                    print('省外：', self.crawler.stats.get_value('sw_num'))
            # 获取下一页
            form1 = response.xpath('//form[@id="form1"]')
            if form1.get():
                # 第一页时全部都有
                __VIEWSTATE = form1.xpath('.//input[@name="__VIEWSTATE"]/@value').get()
                __VIEWSTATEGENERATOR = form1.xpath('.//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
                __EVENTVALIDATION = form1.xpath('.//input[@name="__EVENTVALIDATION"]/@value').get()
                ASPxGridView2_CallbackState = form1.xpath('.//input[@name="ASPxGridView2$CallbackState"]/@value').get()
                self.data_sw['__VIEWSTATE'] = __VIEWSTATE
                self.data_sw['__VIEWSTATEGENERATOR'] = __VIEWSTATEGENERATOR
                self.data_sw['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data_sw['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState
            else:
                # 下一页时，只会变化__EVENTVALIDATION和ASPxGridView2_CallbackState
                __EVENTVALIDATION = re.search('\d+\|(.*?)< ?table', response.text).group(1)
                ASPxGridView2_CallbackState = response.xpath('.//input[@name="ASPxGridView2$CallbackState"]/@value').get()
                self.data_sw['__EVENTVALIDATION'] = __EVENTVALIDATION
                self.data_sw['ASPxGridView2$CallbackState'] = ASPxGridView2_CallbackState
            onclick_attributes = response.xpath('//td[@class="dxpControl"]//td[@class="dxpButton"]/@onclick').getall()
            for onclick_attribute in onclick_attributes:
                if 'PBN' in onclick_attribute:
                    yield scrapy.FormRequest(self.post_url, formdata=self.data_sw, callback=self.parse_sw, priority=1)
                    break
