"""
Record search history and page click history
"""

import pymongo
from datetime import datetime


class Recorder:
    def __init__(self, user_id):
        self.user_id = user_id
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.db = self.mongo_client['mysearchengine']
        collection_names = self.db.list_collection_names()
        if 'history' not in collection_names:
            self.db.create_collection('history')
        self.history_collection = self.db['history']
        if 'page_click' not in collection_names:
            self.db.create_collection('page_click')
        self.page_click_collection = self.db['page_click']
        if self.history_collection.find_one({'user_id': self.user_id}) is None:
            self.history_collection.insert_one({'user_id': self.user_id, 'search': [], 'recent_click': []})
        if 'user_id_1' not in self.history_collection.index_information():
            self.history_collection.create_index([('user_id', pymongo.ASCENDING)])

    def record_search(self, query_str, sort_by_time, filter_site):
        search_history = self.history_collection.find_one({'user_id': self.user_id})['search']
        search_history.append({'query_str': query_str, 'sort_by_time': sort_by_time, 'filter_site': filter_site, 'time': datetime.now()})
        if len(search_history) > 20:
            search_history = search_history[-20:]
        self.history_collection.update_one({'user_id': self.user_id}, {'$set': {'search': search_history}})

    def record_click(self, url):
        recent_click = self.history_collection.find_one({'user_id': self.user_id})['recent_click']
        recent_click_urls = [list(item.keys())[0] for item in recent_click]
        if url in recent_click_urls:
            click_times = recent_click[recent_click_urls.index(url)][url]['click_times'] + 1
            recent_click.remove(recent_click[recent_click_urls.index(url)])
            recent_click.insert(0, {url: {'click_times': click_times, 'time': datetime.now()}})
            recent_click_users = self.page_click_collection.find_one({'url': url})['users']
            if self.user_id not in recent_click_users:
                recent_click_users.insert(0, self.user_id)
            else:
                recent_click_users.remove(self.user_id)
                recent_click_users.insert(0, self.user_id)
            self.page_click_collection.update_one({'url': url}, {'$set': {'users': recent_click_users}})
        else:
            recent_click.insert(0, {url: {'click_times': 1, 'time': datetime.now()}})
            self.page_click_collection.insert_one({'url': url, 'users': [self.user_id]})
        if len(recent_click) > 20:
            recent_click_users = self.page_click_collection.find_one({'url': list(recent_click[-1].keys())[0]})['users']
            if self.user_id in recent_click_users:
                recent_click_users.remove(self.user_id)
            self.page_click_collection.update_one({'url': list(recent_click[-1].keys())[0]}, {'$set': {'users': recent_click_users}})
            recent_click = recent_click[:20]
        self.history_collection.update_one({'user_id': self.user_id}, {'$set': {'recent_click': recent_click}})


if __name__ == '__main__':
    recorder = Recorder(user_id=1)
    recorder.record_click('http://jwc.nankai.edu.cn/')
    print(recorder.history_collection.find_one({'user_id': 1}))
