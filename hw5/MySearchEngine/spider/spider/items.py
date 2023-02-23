"""
This module contains the definition of the items that are scraped by the spider.
"""

import scrapy


class WebPageItem(scrapy.Item):
    url_id = scrapy.Field()
    url = scrapy.Field()
    redirected_from = scrapy.Field()
    title = scrapy.Field()
    type = scrapy.Field()
    pure_text = scrapy.Field()
    anchor = scrapy.Field()
    anchor_forward_text = scrapy.Field()
    anchor_backward_text = scrapy.Field()
    forward_links = scrapy.Field()
    crawl_time = scrapy.Field()
    page_rank = scrapy.Field()

