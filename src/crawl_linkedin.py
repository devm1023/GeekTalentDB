import conf
from crawler import Crawler
from tor import TorProxyList, new_identity
from logger import Logger
import re
import argparse

class LinkedInCrawler(Crawler):
    directory_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/directory/')
    name_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/')
    ukname_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/.+/gb-0-United-Kingdom')

    def __init__(self, site='linkedin', **kwargs):
        if 'request_args' not in kwargs:
            kwargs['request_args'] = {}
        if 'headers' not in kwargs['request_args']:
            kwargs['request_args']['headers'] = {
                'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding' : 'gzip, deflate, sdch',
                'Accept-Language' : 'en-US,en;q=0.8,de;q=0.6',
                'Connection' : 'keep-alive',
                'DNT' : '1',
                'Host' : 'uk.linkedin.com',
                'Upgrade-Insecure-Requests' : '1',
                'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
            }
        kwargs.pop('share_proxies', None)
        Crawler.__init__(self, site, conf.CRAWL_DB, share_proxies=True,
                         **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = bool(doc.xpath('/html/head/title'))
        if not valid:
            leaf = None,
            links = []
        elif cls.directory_url_pattern.match(redirect_url):
            leaf = False
            linktags = doc.xpath(
                '//*[@id="seo-dir"]/div/div[@class="section last"]/div/ul/li/a')
            links = [(tag.get('href'), False) for tag in linktags]
        elif cls.ukname_url_pattern.match(redirect_url):
            leaf = False
            linktags = doc.xpath('//div[@class="profile-card"]/div/h3/a')
            links = [(tag.get('href'), True) for tag in linktags]
        elif cls.name_url_pattern.match(redirect_url):
            leaf = False
            linktags = doc.xpath('//a[@class="country-specific-link"]')
            links = [(tag.get('href'), False) for tag in linktags]
        else:
            leaf = True
            links = []

        return valid, leaf, links


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--proxies-from', default=None,
                        help='Load proxy URLs from text file. '
                        'Default proxy: ')
    parser.add_argument('--max-level', type=int, default=None,
                        help='Maximum click distance from seed websites.')
    parser.add_argument('--recrawl', 
                        help='Recrawl URLs with a timestamp earlier than '
                        'RECRAWL. Format: YYYY-MM-DD')
    parser.add_argument('--leafs-only', action='store_true',
                        help='Only crawl leaf pages.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up. Default: 10')
    parser.add_argument('--limit', type=int,
                        help='Maximum number of URLs to crawl.')
    parser.add_argument('--urls-from', 
                        help='Text file holding the URLs to crawl.')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs. Default: 1')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Max. number of URLs to crawl in one batch. '
                        'Default: 500')
    parser.add_argument('--batch-time', type=int, default=600,
                        help='Max. time (in secs) to crawl one batch. '
                        'Default: 600')
    parser.add_argument('--crawl-rate', type=float, default=None,
                        help='Desired number of requests per second.')
    parser.add_argument('--request-timeout', type=int, default=30,
                        help='Timeout for requests in secs. Default: 30')
    args = parser.parse_args()

    recrawl = None
    if args.recrawl is not None:
        recrawl = datetime.strptime(args.recrawl, '%Y-%m-%d')
    
    logger = Logger()

    headers = {
        'Accept-Encoding' : 'gzip, deflate, br',
        'User-Agent' : 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8',
        'Accept-Language' : 'en-US,en;q=0.5',
        'Connection' : 'keep-alive',
    }
    request_args = {'headers' : headers}

    proxies = [('socks5://127.0.0.1:9050', 'socks5://127.0.0.1:9050')]
    if args.proxies_from is not None:
        proxies = []
        with open(args.proxies_from, 'r') as inputfile:
            for line in inputfile:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                proxy = tuple(line.split())
                if len(proxy) != 2:
                    raise ValueError('Invalid line in proxy file: {0:s}' \
                                     .format(repr(line)))
                proxies.append(proxy)

    crawler = LinkedInCrawler(
        proxies=proxies,
        crawl_rate=args.crawl_rate,
        request_args=request_args,
        request_timeout=args.request_timeout,
        urls_from=args.urls_from,
        leafs_only=args.leafs_only,
        recrawl=recrawl,
        max_level=args.max_level,
        limit=args.limit,
        max_fail_count=args.max_fail_count,
        jobs=args.jobs,
        batch_size=args.batch_size,
        batch_time=args.batch_time,
        logger=logger)

    crawler.crawl()
    
