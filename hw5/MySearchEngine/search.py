"""
Api between front-end and back-end
"""

from query import MyQueryParser
from whoosh.index import open_dir
from whoosh import scoring
from history import Recorder
import pymongo


class MySearcher:
    def __init__(self, recorder):
        self.ix = open_dir('index/whoosh_index')
        self.parser = MyQueryParser()
        self.limit = 100
        self.recorder = recorder
        self.db = pymongo.MongoClient('localhost', 27017)['mysearchengine']
        self.web_page_collection = self.db['web_page']
        self.history_collection = self.db['history']

    class WebPageItem:
        def __init__(self, url, title, pure_text, anchor_backward_text, anchor_forward_text, page_rank, crawl_time, score=0):
            self.url = url
            self.title = title
            self.pure_text = pure_text
            self.anchor_backward_text = anchor_backward_text
            self.anchor_forward_text = anchor_forward_text
            self.page_rank = page_rank
            self.crawl_time = crawl_time
            self.score = score

        def __str__(self):
            return 'url: ' + self.url + '\n' + 'filtered_title: ' + self.title + '\n' + 'pure_text: ' + self.pure_text + '\n' + 'anchor_backward_text: ' + self.anchor_backward_text + '\n' + 'anchor_forward_text: ' + self.anchor_forward_text + '\n' + 'page_rank: ' + str(self.page_rank) + '\n' + 'crawl_time: ' + str(self.crawl_time) + '\n' + 'score: ' + str(self.score) + '\n'

    def basic_search(self, query_str, limit=100):
        results = []
        with self.ix.searcher(weighting=scoring.BM25F) as searcher:
            query = self.parser.parse_query(query_str)
            for result in searcher.search(query, limit=limit):
                results.append(self.WebPageItem(result['url'], result['title'], result['pure_text'], result['anchor_backward_text'], result['anchor_forward_text'], result['page_rank'], result['crawl_time'], result.score))
            max_bm25f_score = max([result.score for result in results])
            max_page_rank_score = max([result.page_rank for result in results])
            for result in results:
                result.score = result.score / max_bm25f_score * 0.7 + result.page_rank / max_page_rank_score * 0.3
            results.sort(key=lambda x: x.score, reverse=True)
        return results

    def search(self, any_keywords, all_keywords='', complete_keywords='', mask_keywords='', sort_by_time=False, filter_site='', personalized=True):
        def get_kind_preference(result_urls):
            cluster_num = len(self.web_page_collection.find().distinct('cluster_type'))
            user_history = self.history_collection.find_one({'user_id': self.recorder.user_id})
            kind_preference = {}
            if 'kind_preference' not in user_history:
                for i in range(1, cluster_num + 1):
                    kind_preference[str(i)] = 1 / cluster_num   # int cannot be key in mongodb
            else:
                kind_preference = user_history['kind_preference']
            result_preference = {}
            for result_url in result_urls:
                kind = self.web_page_collection.find_one({'url': result_url})['cluster_type']
                if str(kind) not in result_preference:
                    result_preference[str(kind)] = 0
                result_preference[str(kind)] += 1
            for kind in result_preference:
                result_preference[str(kind)] /= len(result_urls)
                kind_preference[str(kind)] = kind_preference[str(kind)] * 0.9 + result_preference[str(kind)] * 0.1
            self.history_collection.update_one({'user_id': self.recorder.user_id}, {'$set': {'kind_preference': kind_preference}})
            return kind_preference

        query_str = '(' + any_keywords + ')'
        if all_keywords != '':
            temp_str = ' AND '.join([keyword for keyword in all_keywords.split(' ')])
            query_str += ' AND (' + temp_str + ')'
        if complete_keywords != '':
            temp_str = ''
            keywords = complete_keywords.split(' ')
            for keyword in keywords:
                temp_str += '"' + keyword + '" '
            query_str += ' AND (' + temp_str + ')'
        if mask_keywords != '':
            temp_str = ' AND '.join([keyword for keyword in mask_keywords.split(' ')])
            query_str += ' AND NOT (' + temp_str + ')'
        self.recorder.record_search(query_str, sort_by_time, filter_site)
        if filter_site == '':
            limit = self.limit
        else:
            limit = None
        results = self.basic_search(query_str, limit=limit)
        if filter_site != '':
            temp_results = []
            for result in results:
                if filter_site in result.url:
                    temp_results.append(result)
            results = temp_results
        if sort_by_time:
            results.sort(key=lambda x: x.crawl_time, reverse=True)
        if len(results) > self.limit:
            results = results[:self.limit]
        if personalized:
            kind_preference = get_kind_preference([result.url for result in results])
            for result in results:
                kind = self.web_page_collection.find_one({'url': result.url})['cluster_type']
                result.score = result.score * 0.8 + kind_preference[str(kind)] * 0.2
            results.sort(key=lambda x: x.score, reverse=True)
        return results


if __name__ == '__main__':
    recorder = Recorder(user_id=1)
    searcher = MySearcher(recorder)
    results = searcher.search(any_keywords='计算机', complete_keywords='', mask_keywords='', sort_by_time=False, filter_site='', personalized=True)
    for result in results:
        print(result)
