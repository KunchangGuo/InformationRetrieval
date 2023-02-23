"""
Parse query string and return a list of query terms
"""

from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser, OrGroup, FuzzyTermPlugin


class MyQueryParser:
    def __init__(self):
        self.ix = open_dir('index/whoosh_index')
        self.parser = MultifieldParser(['title', 'pure_text', 'anchor_backward_text', 'anchor_forward_text'], schema=self.ix.schema, group=OrGroup.factory(0.9))
        self.parser.add_plugin(FuzzyTermPlugin)

    def parse_query(self, query_str):
        return self.parser.parse(query_str)
