"""
Scrapy spider for Nankai University website.
"""

import scrapy
import datetime
import re
import pymongo
from spider.items import WebPageItem


class NankaiSpider(scrapy.Spider):
    name = "nankai"
    start_urls = [
        'https://www.nankai.edu.cn/',
    ]
    allowed_domains = [
        'nankai.edu.cn',
    ]
    error_urls = [  # wrong-url list. ref: https://blog.csdn.net/weixin_44444532/article/details/115611057
        'javascript:;',
        'javascript:void(0);',
        '#',
    ]
    month_dict = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12',
    }
    file_type_list = [
        'zip',
        'rar',
        'pdf',
        'doc',
        'docx',
        'xls',
        'xlsx',
        'ppt',
        'pptx',
        'txt',
    ]
    url_id = 1

    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['mysearchengine']
        if 'url' in self.db.list_collection_names():
            self.db.drop_collection('url')
        if 'redirect' in self.db.list_collection_names():
            self.db.drop_collection('redirect')
        self.url_collection = self.db['url']
        self.redirect_collection = self.db['redirect']
        self.url_collection.insert_one({'url_id': self.url_id, 'url': 'https://www.nankai.edu.cn/'})
        self.url_id += 1
        self.url_collection.insert_one({'url_id': self.url_id, 'url': 'http://www.nankai.edu.cn/'})
        self.url_id += 1
        self.redirect_collection.insert_one({'from_url_id': 2, 'to_url_id': 1})
        self.url_collection.create_index('url_id', unique=True)
        self.url_collection.create_index('url', unique=True)
        self.redirect_collection.create_index('from_url_id', unique=True)

    def spider_closed(self, spider):
        self.client.close()

    def is_invalid_url(self, url):  # whether url is not usable and not in allowed domains
        if url in self.error_urls:
            return True
        for allowed_domain in self.allowed_domains:
            if '.' + allowed_domain in url:  # in case of `@nankai.edu.cn`
                return False
        return True

    def parse_time(self, time_str):  # change time from `Sat, 04 Feb 2023 15:12:18 GMT` to `2023-02-04 23:12:18` (GMT+8)
        time_str = time_str.split(',')[1].strip().split(' ')
        year = time_str[2]
        month = self.month_dict[time_str[1]]
        day = time_str[0]
        hour = time_str[3].split(':')[0]
        minute = time_str[3].split(':')[1]
        second = time_str[3].split(':')[2]
        time_str = year + '-' + month + '-' + day + ' ' + hour + ':' + minute + ':' + second
        time_str = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        time_str = time_str + datetime.timedelta(hours=8)
        time_str = time_str.strftime('%Y-%m-%d %H:%M:%S')
        return time_str

    def filter_text(self, text):  # remove symbols from text and return a list
        def filter_str(string):
            string = re.sub(r'[!?/_,$%^*\"\'|{}<>=:;\+\-—！，。？、~@#￥%…&*©（）()\[\]【】‘’“”：；《》]+', ',', string)
            string = re.sub(r'\s+', '', string)
            return string
        result = []
        if text is None:
            return []
        if isinstance(text, str):
            text = filter_str(text).split(',')
            for temp in text:
                if temp != '':
                    result.append(temp)
            return result
        if isinstance(text, list):
            for i in range(len(text)):
                temp = self.filter_text(text[i])
                if len(temp):
                    result.extend(temp)
        return result

    def parse(self, response):
        web_page = WebPageItem()
        url_find_result = self.url_collection.find_one({'url': response.url})
        if url_find_result is None:
            web_page['url_id'] = self.url_id
            self.url_collection.insert_one({'url_id': self.url_id, 'url': response.url})
            self.url_id += 1
        else:
            web_page['url_id'] = url_find_result['url_id']
        web_page['url'] = response.url
        redirect_urls = response.request.meta.get('redirect_urls')
        if redirect_urls is None:
            web_page['redirected_from'] = 0
        else:
            redirect_url_find_result = self.url_collection.find_one({'url': redirect_urls[0]})
            if redirect_url_find_result is None:
                redirect_url_id = self.url_id
                self.url_collection.insert_one({'url_id': redirect_url_id, 'url': redirect_urls[0]})
                web_page['redirected_from'] = redirect_url_id
                self.url_id += 1
            web_page['redirected_from'] = redirect_url_find_result['url_id']
            redirect_edge_find_result = self.redirect_collection.find_one({'from_url_id': web_page['redirected_from'], 'to_url_id': web_page['url_id']})
            if redirect_edge_find_result is None:
                self.redirect_collection.insert_one({'from_url_id': web_page['redirected_from'], 'to_url_id': web_page['url_id']})
        web_page['crawl_time'] = self.parse_time(response.headers.get('Date').decode('utf-8'))
        web_page['page_rank'] = 0.0
        type = re.sub('/', '', response.url.split('.')[-1])
        if not len(response.body) or type in self.file_type_list:  # Error: `Response content isn't text`. ref: https://stackoverflow.com/questions/64971689/raise-attributeerror-response-content-isnt-text-scarpy-proxy-pool-how-to-solv
            web_page['title'] = ''
            web_page['type'] = type
            web_page['pure_text'] = ''
            web_page['anchor'] = {}
            web_page['anchor_forward_text'] = ''
            web_page['anchor_backward_text'] = ''
            web_page['forward_links'] = []
            yield web_page
        else:
            title = response.xpath('/html/head/title/text()').extract_first()
            if title is not None:
                web_page['title'] = title
                web_page['type'] = 'html'
                pure_text = response.xpath('//li/text()').extract() + response.xpath('//p/text()').extract() + response.xpath('//h1/text()').extract() + response.xpath('//h2/text()').extract()
                web_page['pure_text'] = ','.join(self.filter_text(pure_text))
                web_page['anchor'] = {}
                web_page['forward_links'] = []
                web_page['anchor_backward_text'] = ''
                anchor_forward_text = []
                forward_links_text = []
                for anchor in response.xpath('//a'):
                    text = anchor.xpath('./text()').extract_first()
                    href = anchor.xpath('./@href').extract_first()
                    if text is not None:
                        anchor_forward_text.append(text)
                    if href is None or self.is_invalid_url(href):
                        continue
                    href = response.urljoin(href)
                    forward_links_text.append(href)
                    href_id = 0
                    href_find_result = self.url_collection.find_one({'url': href})
                    if href_find_result is None:
                        href_id = self.url_id
                        self.url_collection.insert_one({'url_id': href_id, 'url': href})
                        web_page['forward_links'].append(href_id)
                        self.url_id += 1
                    else:
                        href_id = href_find_result['url_id']
                        web_page['forward_links'].append(href_id)
                    if href_find_result is None:
                        text = ','.join(self.filter_text(text))
                    if text != '' and text is not None:
                        web_page['anchor'][href] = text
                    yield scrapy.Request(href, callback=self.parse)
                web_page['anchor_forward_text'] = ','.join(self.filter_text(anchor_forward_text))
                yield web_page
