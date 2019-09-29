# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import traceback
from .items import AptitudeItem
import psycopg2
import pymongo



class TianjinPipeline(object):
    def open_spider(self, spider):
        self.conn = psycopg2.connect(host='119.3.206.20', port=54321, user='postgres', password='postgres',
                                     database='province')
        self.cursor = self.conn.cursor()
        self.connection = pymongo.MongoClient('119.3.206.20', 27017)
        self.db = self.connection['tianjin']  # ['MONGODB_DB']
        self.collection = self.db['tianjin_company_name']  # settings['MONGODB_COLLECTION']
        self.beian_company = set(msg.get('company_name') for msg in self.collection.find() if 'company_name' in msg)
        print(self.beian_company)

    def insert_into(self, item):
        ite = dict(item)
        sql = "INSERT INTO {} (".format(item.collection)
        v_list = []
        k_list = []
        for key, value in ite.items():
            if value is not None and value != "" and value != 'None':
                sql += "{},"
                v_list.append(ite[key])
                k_list.append(key)
        sql = sql.format(*k_list)[:-1] + ")" + " VALUES ("
        for key, value in ite.items():
            if value is not None and value != "" and value != 'None':
                sql += "'{}',"
        sql = sql.format(*v_list)[:-1] + ")"

        self.cursor.execute(sql)
        self.conn.commit()
        print(item.collection)

    def process_item(self, item, spider):
        cache_item = dict(item)
        if cache_item.get('url') or (cache_item.get('company_name') and cache_item.get('company_name') not in self.beian_company):
            # self.beian_company.add(cache_item.get('corpname'))
            try:
                self.insert_into(item)
                if not isinstance(item, AptitudeItem):
                    self.collection.insert(dict(item))
            except psycopg2.InterfaceError:
                self.open_spider(spider=spider)
                return self.process_item(item, spider)
            except:
                self.conn.rollback()
                traceback.print_exc()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
        self.connection.close()
