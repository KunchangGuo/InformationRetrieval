"""
This module contains the pipelines for the spider.
Stores the scraped data in MongoDB.
"""

from itemadapter import ItemAdapter
import json
from itemadapter import ItemAdapter
from scrapy.exporters import JsonLinesItemExporter
import pymongo


class JsonLinesExporterPipeline(object):
    def open_spider(self, spider):
        self.file = open('../data/search.jsonl', 'wb')
        self.exporter = JsonLinesItemExporter(self.file, ensure_ascii=False)
        self.exporter.start_exporting()

    def close_spider(self, spider):
        self.exporter.finish_exporting()
        self.file.close()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item


class MongoPipeline(object):
    def open_spider(self, spider):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['mysearchengine']
        if 'web_page' in self.db.list_collection_names():
            self.db.drop_collection('web_page')
        self.web_page_collection = self.db['web_page']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.web_page_collection.insert_one(dict(item))
        return item
