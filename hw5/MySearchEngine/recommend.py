"""
ItemCF-based recommend algorithm
"""

from history import Recorder
import pymongo
import random
import numpy as np
from networkx import Graph


class MyRecommender:
    def __init__(self, user_id):
        self.user_id = user_id
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.db = self.mongo_client['mysearchengine']
        self.web_page_collection = self.db['web_page']
        self.history_collection = self.db['history']
        self.page_click_collection = self.db['page_click']
        if 'user_id_1' not in self.history_collection.index_information():
            self.history_collection.create_index([('user_id', pymongo.ASCENDING)])
        if 'search_1' not in self.history_collection.index_information():
            self.history_collection.create_index([('search', pymongo.ASCENDING)])
        if 'recent_click_1' not in self.history_collection.index_information():
            self.history_collection.create_index([('recent_click', pymongo.ASCENDING)])

    def __del__(self):
        self.mongo_client.close()

    class WebPageItem:
        def __init__(self, url, title, score):
            self.url = url
            self.title = title
            self.score = score

        def __str__(self):
            return 'url: ' + self.url + '\ntitle: ' + self.title + '\nscore: ' + str(self.score) + '\n'

    def random_click(self):
        user_id_list = list(range(1, 100))
        url_list = self.web_page_collection.find().distinct('url')
        random.shuffle(url_list)
        url_list = url_list[:100]
        for user_id in user_id_list:
            recorder = Recorder(user_id=user_id)
            for i in range(20):
                recorder.record_click(random.choice(url_list))

    def __del__(self):
        self.mongo_client.close()

    def cal_norm(self):
        urls = self.page_click_collection.find().distinct('url')
        for url in urls:
            users = self.page_click_collection.find_one({'url': url})['users']
            click_times = []
            for user in users:
                recent_click = self.history_collection.find_one({'user_id': user})['recent_click']
                recent_click_urls = [list(item.keys())[0] for item in recent_click]
                if url in recent_click_urls:
                    click_times.append(recent_click[recent_click_urls.index(url)][url]['click_times'])
            vector_norm = np.linalg.norm(click_times, ord=2)
            self.page_click_collection.update_one({'url': url}, {'$set': {'norm': vector_norm}})

    def build_index(self):
        def simularity(page1, page2):
            user_list1 = self.page_click_collection.find_one({'url': page1})['users']
            user_list2 = self.page_click_collection.find_one({'url': page2})['users']
            user_list = list(set(user_list1) & set(user_list2))
            click_times1 = []
            click_times2 = []
            for user in user_list:
                recent_click = self.history_collection.find_one({'user_id': user})['recent_click']
                recent_click_urls = [list(item.keys())[0] for item in recent_click]
                if page1 in recent_click_urls:
                    click_times1.append(recent_click[recent_click_urls.index(page1)][page1]['click_times'])
                if page2 in recent_click_urls:
                    click_times2.append(recent_click[recent_click_urls.index(page2)][page2]['click_times'])
            dot_product = np.dot(click_times1, click_times2)
            norm1 = self.page_click_collection.find_one({'url': page1})['norm']
            norm2 = self.page_click_collection.find_one({'url': page2})['norm']
            return dot_product / (norm1 * norm2)

        urls = self.page_click_collection.find().distinct('url')
        graph = Graph()
        graph.add_nodes_from(urls)
        for url in urls:
            for url2 in urls:
                if url != url2:
                    if graph.has_edge(url, url2):
                        continue
                    graph.add_edge(url, url2, weight=simularity(url, url2))
        for url in urls:  # remove weight
            self.page_click_collection.update_one({'url': url}, {'$set': {'simularity': {}}})
        for edge in graph.edges:
            url1_simularity = self.page_click_collection.find_one({'url': edge[0]})['simularity']
            url2_simularity = self.page_click_collection.find_one({'url': edge[1]})['simularity']
            url1_simularity[edge[1]] = graph.get_edge_data(edge[0], edge[1])['weight']
            url2_simularity[edge[0]] = graph.get_edge_data(edge[0], edge[1])['weight']
            self.page_click_collection.update_one({'url': edge[0]}, {'$set': {'simularity': url1_simularity}})
            self.page_click_collection.update_one({'url': edge[1]}, {'$set': {'simularity': url2_simularity}})

    def recommend(self, limit=20, each_simularity_num=5):
        recent_click = self.history_collection.find_one({'user_id': self.user_id})['recent_click']
        recent_click_urls = [list(item.keys())[0] for item in recent_click]
        simular_pages = {}
        for url in recent_click_urls:
            simularity = self.page_click_collection.find_one({'url': url})['simularity']
            simularity = sorted(simularity.items(), key=lambda x: x[1], reverse=True)
            simularity = simularity[:each_simularity_num]
            for page in simularity:
                if page[0] in simular_pages:
                    simular_pages[page[0]] += page[1]
                else:
                    simular_pages[page[0]] = page[1]
        simular_pages = sorted(simular_pages.items(), key=lambda x: x[1], reverse=True)
        simular_pages = simular_pages[:limit]
        pages = []
        for page in simular_pages:
            title = self.web_page_collection.find_one({'url': page[0]})['title']
            pages.append(self.WebPageItem(url=page[0], title=title, score=page[1]))
        return pages



if __name__ == '__main__':
    recommender = MyRecommender(user_id=1)
    # recommender.random_click()
    # recommender.cal_norm()
    # recommender.build_index()
    result = recommender.recommend()
    for item in result:
        print(item)

