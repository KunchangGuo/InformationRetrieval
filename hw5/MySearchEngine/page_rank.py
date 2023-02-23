"""
PageRank calculated by default damping factor 0.9, max iterations 100, min delta 0.000001
"""

import networkx as nx
import pymongo


class PageRankCalculator:
    def __init__(self):
        self.damping_factor = 0.9  # alpha
        self.damping_value = 0  # (1-alpha)/N
        self.max_iterations = 100
        self.iteration_counter = 0
        self.min_delta = 0.000001
        self.graph = nx.DiGraph()
        self.mongo_client = pymongo.MongoClient('localhost', 27017)
        self.db = self.mongo_client['mysearchengine']
        if 'web_page' not in self.db.list_collection_names():
            print('web_page collection not found')
            return
        self.web_page_collection = self.db['web_page']

    def __del__(self):
        self.mongo_client.close()

    def calculate_page_rank(self):  # add nodes and edges to graph. page rank init with -1. calculate page rank by iteration
        web_page_and_forward_links = self.web_page_collection.aggregate([{'$project': {'url_id': 1, 'forward_links': 1}}])
        for web_page in web_page_and_forward_links:
            home_page_id = web_page['url_id']
            forward_link_ids = web_page['forward_links']
            if home_page_id not in self.graph.nodes():
                self.graph.add_node(home_page_id, page_rank=0)
            for forward_link_id in forward_link_ids:
                if forward_link_id not in self.graph.nodes():
                    self.graph.add_node(forward_link_id, page_rank=0)
                self.graph.add_edge(home_page_id, forward_link_id)
        graph_size = len(self.graph.nodes())
        if not graph_size:
            print('graph size is 0. exit.')
            return
        self.damping_value = (1 - self.damping_factor) / graph_size
        for node in self.graph.nodes():
            self.graph.nodes[node]['page_rank'] = 1 / graph_size
            if self.graph.out_degree(node) == 0:   # if node has no out degree, add edges to all nodes.
                for another_node in self.graph.nodes():
                    self.graph.add_edge(node, another_node)
        for i in range(self.max_iterations):
            delta = self.iterate()
            self.iteration_counter += 1
            if delta < self.min_delta:
                break
        for node in self.graph.nodes():
            self.web_page_collection.update_one({'url_id': node}, {'$set': {'page_rank': self.graph.nodes[node]['page_rank']}})
        print('page rank calculation finished. iteration: {}, delta: {}'.format(self.iteration_counter, delta))

    def iterate(self):
        delta = 0
        for node in self.graph.nodes():
            rank = 0
            for in_node in list(self.graph.in_edges(node)):  # translate to list. ref: https://stackoverflow.com/questions/47325667/object-is-not-subscripable-networkx
                in_node = in_node[0]
                rank += self.graph.nodes[in_node]['page_rank'] / self.graph.out_degree(in_node)
            rank = self.damping_value + self.damping_factor * rank
            delta = abs(self.graph.nodes[node]['page_rank'] - rank)
            self.graph.nodes[node]['page_rank'] = rank
        return delta
