import conf
from crawler import Crawler
from tor import TorProxyList, new_identity
from logger import Logger
import re
import argparse

class TorCrawler(Crawler):
    def __init__(self, site, database=conf.CRAWL_DB, nproxies=1,
                 tor_base_port=13000, tor_timeout=60, tor_retries=3,
                 tor_password='', tor_hashed_password='', **kwargs):
        Crawler.__init__(self, site, database, **kwargs)
        self.add_config(nproxies=nproxies,
                        tor_base_port=tor_base_port,
                        tor_timeout=tor_timeout,
                        tor_retries=tor_retries)
        self.set_config(
            proxies=['socks5://127.0.0.1:{0:d}'.format(tor_base_port+2*i) \
                     for i in range(nproxies)])
        self.tor_proxies = None

    @classmethod
    def on_visit(cls, iproxy, proxy, proxy_state, valid):
        if not valid:
            port = int(proxy.split(':')[1])
            new_identity(port=port+1, password=conf.TOR_PASSWORD)

    def init_proxies(self, config):
        self.tor_proxies \
            = TorProxyList(len(config['proxies']),
                           base_port=config['tor_base_port'],
                           restart_after=config['tor_timeout'],
                           max_restart=config['tor_retries'],
                           hashed_password=conf.TOR_HASHED_PASSWORD)
        config['logger'].log('Tor proxies started.\n')
        return [None]*len(config['proxies'])

    def finish_proxies(self, config, proxy_states):
        self.tor_proxies.kill()
        self.tor_proxies = None
        
    def on_timeout(self, config, proxy_states):
        self.finish_proxies(config, proxy_states)
        return self.init_proxies(self, config)
        

class LinkedInCrawler(TorCrawler):
    directory_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/directory/')
    name_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/')
    ukname_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/.+/gb-0-United-Kingdom')

    def __init__(self, site='linkedin', **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {
                'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding' : 'gzip, deflate, sdch',
                'Accept-Language' : 'en-US,en;q=0.8,de;q=0.6',
                'Connection' : 'keep-alive',
                'DNT' : '1',
                'Host' : 'uk.linkedin.com',
                'Upgrade-Insecure-Requests' : '1',
                'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
            }
        TorCrawler.__init__(self, site, **kwargs)
    
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
    parser.add_argument('--proxies', type=int, default=1,
                        help='Number of Tor proxies to start. Default: 1')
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
    parser.add_argument('--tor-base-port', type=int, default=13000,
                        help='Smallest port for Tor proxies. Default: 13000')
    parser.add_argument('--tor-timeout', type=int, default=60,
                        help='Timeout in secs for starting Tor process. '
                        'Default: 60')
    parser.add_argument('--tor-retries', type=int, default=3,
                        help='Number of retries for starting Tor process. '
                        'Default: 3')
    args = parser.parse_args()

    recrawl = None
    if args.recrawl is not None:
        recrawl = datetime.strptime(args.recrawl, '%Y-%m-%d')
    
    logger = Logger()
    
    crawler = LinkedInCrawler(
        crawl_rate=args.crawl_rate,
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
        nproxies=args.proxies,
        tor_base_port=args.tor_base_port,
        tor_timeout=args.tor_timeout,
        tor_retries=args.tor_retries,
        logger=logger)

    crawler.crawl()
    
