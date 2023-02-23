"""
get search results and record the search history
"""

from history import Recorder
from search import MySearcher
from recommend import MyRecommender

if __name__ == '__main__':
    user_id = 1
    recorder = Recorder(user_id)
    searcher = MySearcher(recorder)
    recommender = MyRecommender(user_id)
    search_results = searcher.search(any_keywords='计算机学院', all_keywords='', complete_keywords='', mask_keywords='', sort_by_time=False,
                              filter_site='', personalized=True)
    recommend_results = recommender.recommend()
    print('search results:')
    for result in search_results:
        print(result)
    print('recommend results:')
    for result in recommend_results:
        print(result)
