# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2
import traceback

import pymongo


class ShanghaiPipeline(object):
    def open_spider(self, spider):
        self.conn = psycopg2.connect(host='119.3.206.20', port=54321, user='postgres', password='postgres',
                                     database='province')
        self.cursor = self.conn.cursor()

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

    def process_item(self, item, spider):
        try:
            self.insert_into(item)
        except:
            self.conn.rollback()
            traceback.print_exc()
            self.connection = pymongo.MongoClient('119.3.206.20', 27017)
            self.db = self.connection['error']  # ['MONGODB_DB']
            self.collection = self.db['error_msg']  # settings['MONGODB_COLLECTION']
            self.collection.insert(dict(item))
            self.connection.close()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
