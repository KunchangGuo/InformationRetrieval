"""
This module is used to create index for the search engine.
1. define the schema of the index
2. correct redirections
3. implement backlink and page rank
4. create index
"""

import re
import pymongo
import os
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID, DATETIME, NUMERIC
from jieba.analyse import ChineseAnalyzer
from datetime import datetime
from page_rank import PageRankCalculator
from hashlib import md5


class Indexer:
    def __init__(self):
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.db = self.mongo_client['mysearchengine']
        collection_names = self.db.list_collection_names()
        if 'url' not in collection_names:
            print('url collection not found')
            return
        if 'redirect' not in collection_names:
            print('redirect collection not found')
            return
        if 'web_page' not in collection_names:
            print('web_page collection not found')
            return
        self.url_collection = self.db['url']
        self.redirect_collection = self.db['redirect']
        self.web_page_collection = self.db['web_page']
        if 'url_id_1' not in self.url_collection.index_information():
            self.url_collection.create_index([('url_id', pymongo.ASCENDING)])
        if 'forward_links_1' not in self.web_page_collection.index_information():
            self.web_page_collection.create_index([('forward_links', pymongo.ASCENDING)])
        self.analyzer = ChineseAnalyzer()
        self.schema = Schema(
            url=ID(unique=True, stored=True),
            title=TEXT(stored=True, analyzer=self.analyzer),
            pure_text=TEXT(stored=True, analyzer=self.analyzer),
            anchor_backward_text=TEXT(stored=True, analyzer=self.analyzer),
            anchor_forward_text=TEXT(stored=True, analyzer=self.analyzer),
            page_rank=NUMERIC(stored=True),
            crawl_time=DATETIME(stored=True)
        )
        if not os.path.exists('index/whoosh_index'):
            os.mkdir('index/whoosh_index')
        self.ix = create_in('index/whoosh_index', self.schema)
        self.writer = self.ix.writer()
        self.page_rank_calculator = PageRankCalculator()

    def __del__(self):
        self.mongo_client.close()

    def correct_redirect(self):  # essential correction for page rank algorithm
        redirect_edges = self.redirect_collection.find()
        for edge in redirect_edges:
            redirect_forward_links_list = self.web_page_collection.find({'forward_links': {'$in': [edge['from_url_id']]}})
            for forward_links in redirect_forward_links_list:
                new_forward_links = (set(forward_links['forward_links']) - {edge['from_url_id']}) | {edge['to_url_id']}
                self.web_page_collection.update_one({'url_id': forward_links['url_id']}, {'$set': {'forward_links': list(new_forward_links)}})

    def implement_backward_anchor_text(self):  # backward anchor text helps with searching anchor href
        def filter_str(str):  # remove all whitespace. although filtered in spider, there are still some whitespace
            return re.sub(r'\s+', '', str)

        def get_type(url):
            return re.sub(r'/', '', url.split('.')[-1])

        web_page_ids = [web_page_id['url_id'] for web_page_id in self.web_page_collection.aggregate([{'$project': {'url_id': 1}}])]
        for web_page_id in web_page_ids:
            web_page = self.web_page_collection.find_one({'url_id': web_page_id})
            anchor = web_page['anchor']
            for href in anchor:
                text = filter_str(anchor[href])
                href_id = self.url_collection.find_one({'url': href})['url_id']
                redirect = self.redirect_collection.find_one({'from_url_id': href_id})
                redirect_from = 0
                if redirect is not None:
                    redirect_from = href_id
                    href_id = redirect['to_url_id']
                if href_id in web_page_ids:
                    if text == '':
                        continue
                    anchor_backward_text = self.web_page_collection.find_one({'url_id': href_id})['anchor_backward_text']
                    if anchor_backward_text == '':
                        new_anchor_backward_text = text
                    else:
                        anchor_backward_text_list = anchor_backward_text.split(',')
                        if text in anchor_backward_text_list:
                            continue
                        new_anchor_backward_text = anchor_backward_text + ',' + text
                    self.web_page_collection.update_one({'url_id': href_id}, {'$set': {'anchor_backward_text': new_anchor_backward_text}})

    def collapse_the_same(self):  # eliminate same page. md5 should be calculated in spider, but I found it too late
        def get_md5(str):
            return md5(str.encode('utf-8')).hexdigest()

        web_page_ids = [web_page_id['url_id'] for web_page_id in self.web_page_collection.aggregate([{'$project': {'url_id': 1}}])]
        for web_page_id in web_page_ids:
            web_page = self.web_page_collection.find_one({'url_id': web_page_id})
            page_md5 = get_md5(web_page['title'] + web_page['pure_text'] + web_page['anchor_backward_text'] + web_page['anchor_forward_text'])
            self.web_page_collection.update_one({'url_id': web_page_id}, {'$set': {'md5': page_md5}})
        url_id_lists = [url_id['url_id'] for url_id in self.web_page_collection.aggregate([{'$group': {'_id': '$md5', 'url_id': {'$push': '$url_id'}, 'count': {'$sum': 1}}}, {'$match': {'count': {'$gt': 1}}}, {'$project': {'url_id': 1}}])]
        for url_id_list in url_id_lists:
            url_id_to_keep = url_id_list[0]
            url_id_to_delete = url_id_list[1:]
            print('keep:', url_id_to_keep, 'replace:', url_id_to_delete)
            for url_id in url_id_to_delete:
                forward_links_list = self.web_page_collection.find({'forward_links': {'$in': [url_id]}})
                for forward_links in forward_links_list:
                    new_forward_links = (set(forward_links['forward_links']) - {url_id}) | {url_id_to_keep}
                    self.web_page_collection.update_one({'url_id': forward_links['url_id']}, {'$set': {'forward_links': list(new_forward_links)}})
                self.web_page_collection.delete_one({'url_id': url_id})

    def build_index(self):
        web_page_ids = [web_page_id['url_id'] for web_page_id in self.web_page_collection.aggregate([{'$project': {'url_id': 1}}])]
        doc_counter = 0
        for web_page_id in web_page_ids:
            web_page = self.web_page_collection.find_one({'url_id': web_page_id})
            self.writer.add_document(
                url=web_page['url'],
                title=web_page['title'],
                pure_text=web_page['pure_text'],
                anchor_backward_text=web_page['anchor_backward_text'],
                anchor_forward_text=web_page['anchor_forward_text'],
                page_rank=web_page['page_rank'],
                crawl_time=datetime.strptime(web_page['crawl_time'], '%Y-%m-%d %H:%M:%S')
            )
            doc_counter += 1
            if doc_counter % 1024 == 0:
                self.writer.commit()
                self.writer = self.ix.writer()
        if doc_counter % 1024 != 0:
            self.writer.commit()


if __name__ == '__main__':
    indexer = Indexer()
    indexer.correct_redirect()
    indexer.implement_backward_anchor_text()
    indexer.collapse_the_same()
    indexer.page_rank_calculator.calculate_page_rank()
    indexer.build_index()
