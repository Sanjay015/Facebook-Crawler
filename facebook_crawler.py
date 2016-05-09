"""A Facebook Data Crawler."""
from Architecture import FBCrawl
import yaml
import urllib

config_file = 'config.yaml'
config = yaml.load(open(config_file).read())

url = urllib.urlencode(config)
CRAWLER = FBCrawl()
flag, graph = CRAWLER.fb_auth(url)
page = 'facebook page id'
since = '2015-09-10 00:00:00'
params = 'from, message, updated_time, created_time, shares, status_type,\
          type, picture, link'
if flag:
    data = CRAWLER.get_updated_posts(graph, page, since, params)
    print data
