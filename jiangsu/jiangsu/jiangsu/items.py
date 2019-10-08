# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanyItem(scrapy.Item):
    collection = "company_msg"
    province_company_id = scrapy.Field()  # 企业id
    social_credit_code = scrapy.Field()  # 社会信用代码/营业执照注册号
    company_name = scrapy.Field()  # 企业名称
    url = scrapy.Field()  # url
    ceoname = scrapy.Field()  # 企业负责人
    ctoname = scrapy.Field()  # 技术负责人
    regis_address = scrapy.Field()  # 企业注册地址
    leal_person = scrapy.Field()  # 企业法人代表
    business_address = scrapy.Field()  # 企业经营地址
    regis_type = scrapy.Field()  # 经济性质
    registered_capital = scrapy.Field()  # 注册资本金（万元）
    build_date = scrapy.Field()  # 企业成立日期
    contact_person = scrapy.Field()  # 联系人
    leal_person_title = scrapy.Field()  # 法人代表职称
    postalcode = scrapy.Field()  # 邮政编码
    contact_phone = scrapy.Field()  # 联系人电话
    contact_address = scrapy.Field()  # 联系地址
    leal_person_duty = scrapy.Field()  # 法人代表职务
    fax = scrapy.Field()  # 传真号码
    tel = scrapy.Field()  # 办公电话
    website = scrapy.Field()  # 企业网址
    email = scrapy.Field()  # 电子邮箱
    reg_address_province = scrapy.Field()  # 注册省
    reg_address_country = scrapy.Field()  # 注册县
    contact_tel = scrapy.Field()  # 联系人办公电话
    enginer = scrapy.Field()  # 总工程师
    department = scrapy.Field()  # 管理部门/所属区县
    danweitype = scrapy.Field()  # 企业类型(建筑业)
    area_code = scrapy.Field()  # 所属区域
    source = scrapy.Field()  # 来源
    status = scrapy.Field()  # 状态, 默认1
    create_time = scrapy.Field()  # 创建日期
    modification_time = scrapy.Field()  # 修改日期
    is_delete = scrapy.Field()  # 是否删除, 默认0


class AptitudeItem(scrapy.Item):
    collection = "aptitude_msg"
    company_name = scrapy.Field()
    province_company_id = scrapy.Field()  # 企业id
    aptitude_id = scrapy.Field()  # 资质证书编号
    aptitude_organ = scrapy.Field()  # 发证机关
    aptitude_startime = scrapy.Field()  # 资质证书发证日期
    aptitude_endtime = scrapy.Field()  # 资质证书有效截止日期
    aptitude_name = scrapy.Field()  # 资质项名称
    level = scrapy.Field()  # 资质等级
    check_time = scrapy.Field()  # 核准时间
    aptitude_small = scrapy.Field()  # 资质小类
    aptitude_credit_level = scrapy.Field()  # 资质信用等级
    aptitude_major = scrapy.Field()  # 资质专业
    aptitude_large = scrapy.Field()  # 资质大类
    aptitude_ser = scrapy.Field()  # 资质证书序列号
    approval_number = scrapy.Field()  # 批准文号
    aptitude_usefultime = scrapy.Field()  # 证书有效期
    aptitude_type = scrapy.Field()  # 资质类别
    source = scrapy.Field()  # 来源
    status = scrapy.Field()  # 状态, 默认1
    create_time = scrapy.Field()  # 创建日期
    modification_time = scrapy.Field()  # 修改日期
    is_delete = scrapy.Field()  # 是否删除, 默认0


class BeiAnItem(scrapy.Item):
    collection = "beian_copy1"
    company_name = scrapy.Field()  # 企业名称
    social_credit_code = scrapy.Field()  # 社会信用代码
    record_province = scrapy.Field()  # 备案省份
    status = scrapy.Field()  # 状态, 默认1
    create_time = scrapy.Field()  # 创建日期
    modification_time = scrapy.Field()  # 修改日期
    is_delete = scrapy.Field()  # 是否删除, 默认0
