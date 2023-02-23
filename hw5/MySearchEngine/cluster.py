# use hanlp to make k-means clustering
from pyhanlp import *
import pymongo


ClusterAnalyzer = JClass('com.hankcs.hanlp.mining.cluster.ClusterAnalyzer')


class Cluster:
    def __init__(self):
        self.analyzer = ClusterAnalyzer()
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.db = self.mongo_client['mysearchengine']
        self.web_page_collection = self.db['web_page']

    def __del__(self):
        self.mongo_client.close()

    def cluster(self):
        for page in self.web_page_collection.find():
            self.analyzer.addDocument(page['url_id'], page['title'] + page['pure_text'] + page['anchor_backward_text'] + page['anchor_forward_text'])
        results = self.analyzer.repeatedBisection(1.0)
        print(len(results))
        type_count = 1
        for result in results:
            cluster_type = type_count
            type_count += 1
            for url_id in result:
                self.web_page_collection.update_one({'url_id': url_id}, {'$set': {'cluster_type': cluster_type}})


if __name__ == '__main__':
    cluster = Cluster()
    cluster.cluster()
