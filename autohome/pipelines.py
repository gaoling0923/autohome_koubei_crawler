# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import codecs

import pymongo
from _md5 import md5

import happybase
from scrapy.conf import settings
from autohome.items import KoubeiItem, KoubeiFailedItem, KoubeiUrlItem
import pymysql

class AutohomePipeline(object):
    def process_item(self, item, spider):
        return item
class HBasePipelinef(object):
    def __init__(self):
        host = settings['HBASE_HOST']
        table_name = settings['HBASE_TABLE']
        port = settings['HBASE_PORT']
        self.connection = happybase.Connection(host)
        self.table = self.connection.table(table_name)

    def process_item(self, item, spider):
        # cl = dict(item)

        if isinstance(item, koubeiItem):
            # self.table.put('text', cl)
            print('进入pipline')
            spec_name = item['spec_name']
            address = item['address']
            buy_date = item['buy_date']
            buy_price = item['buy_price']
            space = item['space']
            power = item['power']
            manipulation = item['manipulation']
            fuel = item['fuel']
            comfort = item['comfort']
            surface = item['surface']
            trim = item['trim']
            ratio = item['ratio']
            purpose = item['purpose']
            title = item['title']
            pscore = item['pscore']
            percount = item['percount']
            content = item['content']
            # sunumber = item['sunumber']
            # lookcount = item['lookcount']
            # crawldate = item['crawldate']

            self.table.put(md5(spec_name.encode('utf-8') + buy_date.encode('utf-8')).hexdigest(), {'cf1:spec_name':spec_name,
                                                                                                'cf1:address':address,
                                                                                                'cf1:buy_date':buy_date,
                                                                                                'cf1:buy_price':buy_price,
                                                                                                'cf1:space':space,
                                                                                                'cf1:power':power,
                                                                                                'cf1:manipulation':manipulation,
                                                                                                'cf1:fuel':fuel,
                                                                                                'cf1:comfort':comfort,
                                                                                                'cf1:surface':surface,
                                                                                                'cf1:trim':trim,
                                                                                                'cf1:ratio':ratio,
                                                                                                'cf1:purpose': purpose,
                                                                                                'cf1:content': content,
                                                                                                # 'cf1:title': title,
                                                                                                # 'cf1:pscore': pscore,
                                                                                                # 'cf1:percount': percount,
                                                                                                # 'cf1:sunumber': sunumber,
                                                                                                # 'cf1:lookcount': lookcount,
                                                                                                # 'cf1:crawldate':crawldate,
                                                                                                })

        return item
class MongoDBPipeline(object):

    def __init__(self):
        host = settings['MONGODB_HOST']
        port = settings['MONGODB_PORT']
        dbName = settings['MONGODB_DBNAME']
        client = pymongo.MongoClient(host=host, port=port)
        tdb = client[dbName]
        self.post = tdb[settings['MONGODB_DOCNAME']]

    def process_item(self, item, spider):
        bbsdetail = dict(item)
        self.post.insert(bbsdetail)
        return item
class KoubeiPipeline(object):
    def __init__(self):
        pass

    def process_item(self, item, spider):
        if isinstance(item, KoubeiItem):
            # file = codecs.open('data/koubei/series/%s.jl'%(item['series_name']), 'a', encoding='utf-8')
            with open('data/koubei/series/%s.jl'%(item['series_name']),'a') as file:
                line = json.dumps(dict(item), ensure_ascii=False) + "\n"
                file.write(line)
            return item
        elif isinstance(item, KoubeiFailedItem):
            with open('data/koubei/failed/koubei_failed_url.jl', 'a') as file:
                line = json.dumps(dict(item), ensure_ascii=False) + "\n"
                file.write(line)
            return item
        else:
            pass

    def spider_closed(self, spider):
        pass


class KoubeiUrlPipeline(object):
    def __init__(self):
        self.conn = pymysql.connect(host='127.0.0.1', user='spider', passwd='spider', db='spider', charset='utf8')
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        if isinstance(item, KoubeiUrlItem):
            sql = ("insert into koubei_url_1(url,series_id,batch) values(%s,%s,%s)")
            lis = (item['url'],item['series_id'], '1')
            status = self.cur.execute(sql, lis)
            return item
        else:
            pass

    def spider_closed(self, spider):
        self.conn.close()
        self.cur.close()


