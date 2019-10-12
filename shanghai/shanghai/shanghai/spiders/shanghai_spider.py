# -*- coding: utf-8 -*-
import datetime
import time

import scrapy
import re

from scrapy import signals

from ..items import CompanyItem, BeiAnItem, AptitudeItem


class ShanghaiSpiderSpider(scrapy.Spider):
    name = 'shanghai_spider'
    allowed_domains = ['ciac.zjw.sh.gov.cn']
    get_url = 'https://ciac.zjw.sh.gov.cn/CreditManualNew/PublicCompany/ShIndex?'
    post_url = 'https://ciac.zjw.sh.gov.cn/CreditManualNew/PublicCompany/SearchList'
    basic_msg_url = 'https://ciac.zjw.sh.gov.cn/CreditManualNew/PublicCompany/Qyxx'
    aptitude_msg_url = 'https://ciac.zjw.sh.gov.cn/CreditManualNew/PublicCompany/Zzqk'
    data = {
        'dwmc': '',
        'qylx': '1',
        'zzdl': '',
        'zzxl': 'null',
        'zzlb': 'null',
        'zzdj': 'null',
        'page': '1'
    }
    data_sw = {
        'dwmc': '',
        'qylx': '2',
        'zzdl': '',
        'zzxl': 'null',
        'zzlb': 'null',
        'zzdj': 'null',
        'page': '1'
    }

    @staticmethod
    def get_province_company_id():
        return 'shanghai_' + str(int(time.time()*1000))

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
        yield scrapy.FormRequest(self.post_url, formdata=self.data, meta={'page': 1}, dont_filter=True)
        yield scrapy.FormRequest(self.post_url, formdata=self.data_sw, meta={'page': 1}, dont_filter=True, callback=self.parse_sw)

    def parse(self, response):
        # 解析每页的onclick链接
        onclicks = response.xpath('//table//tr/td[2]/a/@onclick').getall()
        companies = response.xpath('//table//tr/td[2]/a/text()').getall()
        if len(onclicks) == len(companies):
            print(response.request.body)
            for company_name, onclick in zip(companies, onclicks):
                dwid, dwdm = re.findall('openwindow\((.*?), ?(.*?),', onclick)[0]
                url = self.get_url + 'dwid=' + dwid + '&dwdm=' + dwdm
                province_company_id = self.get_province_company_id()  # 生成此刻的企业id
                yield scrapy.FormRequest(self.basic_msg_url,
                                         formdata={'dwid': dwid},
                                         meta={'company_url': url,
                                               'province_company_id': province_company_id,
                                               'company_name': company_name},
                                         callback=self.parse_basic_msg)
                yield scrapy.FormRequest(self.aptitude_msg_url,
                                         formdata={'dwid': dwid},
                                         meta={'province_company_id': province_company_id,
                                               'company_name': company_name},
                                         callback=self.parse_aptitude_msg)
        else:
            with open('error.txt', 'a') as f:
                f.write(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.body}--len(onclicks) == len(companies)\n")
                f.flush()

        # 发送所有页数
        if response.meta.get('page') == 1:
            current_page, total_items, total_pages = re.findall('CurrentPage: ?(\d+).*?TotalItems: ?(\d+).*?TotalPages: ?(\d+)', response.text, re.S)[0]
            print(current_page, total_items, total_pages)
            for i in range(2, int(total_pages)+1):
                self.data['page'] = str(i)
                yield scrapy.FormRequest(self.post_url, formdata=self.data, priority=int(total_pages)+1-i)

    def parse_basic_msg(self, response):
        time_now = self.get_create_time()  # 生成此刻的时间戳
        regis_type = str(response.xpath('//table//table[1]//tr[2]/td[2]/text()').get()).strip()      # 经济性质
        regis_address = str(response.xpath('//table//table[1]//tr[3]/td[2]/text()').get()).strip()      # 注册地址
        if '：' in regis_address and '\xa0' in regis_address:
            regis_address = regis_address.split('\xa0')[0].split('：')[1] + ' ' + regis_address.split('\xa0')[1].split('：')[1]
        build_date = str(response.xpath('//table//table[1]//tr[4]/td[2]/text()').get()).strip()      # 建立时间

        yield CompanyItem(province_company_id=response.meta['province_company_id'],
                          company_name=response.meta['company_name'],
                          regis_type=regis_type, regis_address=regis_address,
                          build_date=build_date, url=response.meta['company_url'],
                          area_code=310000, source='上海', status=1, create_time=time_now, modification_time=time_now,
                          is_delete=0)
        self.crawler.stats.inc_value('sn_num')
        print('省内：', self.crawler.stats.get_value('sn_num'))

    def parse_aptitude_msg(self, response):
        aptitudes_type = response.xpath('//b/text()').getall()
        tables = response.xpath('//table//table')
        if len(aptitudes_type) == len(tables):
            time_now = self.get_create_time()  # 生成此刻的时间戳
            for i, table in enumerate(tables):
                aptitude_type = aptitudes_type[i].strip()       # 资质类别
                trs = table.xpath('.//tr')
                for j, tr in enumerate(trs):
                    if j == 0:
                        # 表头行， 确定内容行
                        ths = [th.strip() for th in tr.xpath('./th/text()').getall()]
                        print(ths)
                        continue
                    aptitude_small = str(tr.xpath(f'./td[{str(ths.index("类别")+1)}]/text()').get()).strip()     # 资质小类
                    level = str(tr.xpath(f'./td[{str(ths.index("等级")+1)}]/text()').get()).strip()     # 等级
                    aptitude_id = str(tr.xpath(f'./td[{str(ths.index("证书编号")+1)}]/text()').get()).strip()     # 证书编号
                    aptitude_startime = str(tr.xpath(f'./td[{str(ths.index("批准日期")+1)}]/text()').get()).strip()     # 开始日期
                    aptitude_endtime = str(tr.xpath(f'./td[{str(ths.index("有效日期")+1)}]/text()').get()).strip()     # 到期

                    yield AptitudeItem(province_company_id=response.meta['province_company_id'],
                                       company_name=response.meta['company_name'],
                                       aptitude_type=aptitude_type, aptitude_small=aptitude_small,
                                       level=level, aptitude_id=aptitude_id,
                                       aptitude_startime=aptitude_startime, aptitude_endtime=aptitude_endtime,
                                       source='上海', status=1, create_time=time_now, modification_time=time_now, is_delete=0
                                       )
        else:
            with open('error.txt', 'a') as f:
                f.write(
                    f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]-{response.body}--len(aptitudes_type) == len(tables)\n")
                f.flush()

    def parse_sw(self, response):
        companies = response.xpath('//table//tr/td[2]/a/text()').getall()
        time_now = self.get_create_time()  # 生成此刻的时间戳
        for company_name in companies:
            yield BeiAnItem(company_name=company_name, record_province='上海',
                            status=1, create_time=time_now, modification_time=time_now, is_delete=0)
            self.crawler.stats.inc_value('sw_num')
            print('省外：', self.crawler.stats.get_value('sw_num'))
        # 发送所有页数
        if response.meta.get('page') == 1:
            current_page, total_items, total_pages = re.findall('CurrentPage: ?(\d+).*?TotalItems: ?(\d+).*?TotalPages: ?(\d+)', response.text, re.S)[0]
            print(current_page, total_items, total_pages)
            for i in range(2, int(total_pages) + 1):
                self.data_sw['page'] = str(i)
                yield scrapy.FormRequest(self.post_url, formdata=self.data_sw, priority=int(total_pages) + 1 - i, callback=self.parse_sw)
