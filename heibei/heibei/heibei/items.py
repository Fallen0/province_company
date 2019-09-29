# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanyItem(scrapy.Item):
    collection = "company_msg"
    province_company_id = scrapy.Field()  # 企业id
    company_name = scrapy.Field()  # 企业名称
    area_code = scrapy.Field()  # 所属区域
    social_credit_code = scrapy.Field()  # 社会信用代码
    leal_person = scrapy.Field()  # 企业法人代表
    regis_type = scrapy.Field()  # 工商注册类型
    build_date = scrapy.Field()  # 企业成立日期
    reg_address = scrapy.Field()  # 企业注册地
    business_address = scrapy.Field()  # 企业经营地址
    url = scrapy.Field()  # url
    source = scrapy.Field()  # 来源
    status = scrapy.Field()  # 状态，默认1
    create_time = scrapy.Field()  # 创建时间
    modification_time = scrapy.Field()  # 修改时间
    is_delete = scrapy.Field()  # 是否删除bool，默认0


class AptitudeItem(scrapy.Item):
    collection = "aptitude_msg"
    province_company_id = scrapy.Field()  # 企业id
    company_name = scrapy.Field()  # 企业名称
    aptitude_type = scrapy.Field()  # 资质类别
    aptitude_name = scrapy.Field()  # 资质项名称
    aptitude_startime = scrapy.Field()  # 资质证书发证日期
    aptitude_endtime = scrapy.Field()  # 资质证书有效截止日期
    aptitude_id = scrapy.Field()  # 证书编号
    aptitude_organ = scrapy.Field()  # 发证机关
    aptitude_large = scrapy.Field()  # 资质大类
    aptitude_small = scrapy.Field()  # 资质小类
    aptitude_major = scrapy.Field()  # 资质专业
    level = scrapy.Field()  # 资质等级
    aptitude_usefultime = scrapy.Field()  # 证书有效期
    source = scrapy.Field()  # 来源
    status = scrapy.Field()  # 状态，默认1
    create_time = scrapy.Field()  # 创建时间
    modification_time = scrapy.Field()  # 修改时间
    is_delete = scrapy.Field()  # 是否删除bool，默认0


class BeiAnItem(scrapy.Item):
    collection = "beian_copy1"
    company_name = scrapy.Field()  # 企业名称
    social_credit_code = scrapy.Field()  # 社会信用代码
    record_province = scrapy.Field()  # 备案省份
    status = scrapy.Field()  # 状态，默认1
    create_time = scrapy.Field()  # 创建时间
    modification_time = scrapy.Field()  # 修改时间
    is_delete = scrapy.Field()  # 是否删除bool，默认0
