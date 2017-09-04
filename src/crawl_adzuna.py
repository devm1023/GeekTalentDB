"""Crawl adzuna jobs

"""

from adzuna_crawler import AdzunaCrawler
from tor_crawler import TorCrawler
from parse_datetime import parse_datetime
from logger import Logger
import argparse


class AdzunaTorCrawler(TorCrawler, AdzunaCrawler):
    def __init__(self, site='adzuna', nproxies=1, tor_base_port=13000,
                 tor_timeout=60, tor_retries=3, **kwargs):
        TorCrawler.__init__(self, site,
                            nproxies=nproxies,
                            tor_base_port=tor_base_port,
                            tor_timeout=tor_timeout,
                            tor_retries=tor_retries)
        kwargs['proxies'] = self.proxies
        AdzunaCrawler.__init__(self, site=site, **kwargs)
        self.share_proxies = False

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--proxies-from', default=None,
                        help='Load proxy URLs from text file. '
                        'Uses Tor if not specified.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up. Default: 10')
    parser.add_argument('--types', default=None,
                        help='Comma-separated list of page types to crawl. '
                        'If not specified all pages are crawled.')
    parser.add_argument('--exclude-types', default=None,
                        help='Comma-separated list of page types to exclude '
                        'from crawl.')
    parser.add_argument('--recrawl', 
                        help='Recrawl URLs with a timestamp earlier than '
                        'RECRAWL.')
    parser.add_argument('--limit', type=int,
                        help='Maximum number of URLs to crawl.')
    parser.add_argument('--urls-from', 
                        help='Text file holding the URLs to crawl.')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs. Default: 1')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Max. number of URLs to crawl in one batch. '
                        'Computed from crawl rate if omitted.')
    parser.add_argument('--batch-time', type=int, default=600,
                        help='Max. time (in secs) to crawl one batch. '
                        'Default: 600')
    parser.add_argument('--crawl-rate', type=float, default=None,
                        help='Desired number of requests per second.')
    parser.add_argument('--request-timeout', type=int, default=30,
                        help='Timeout for requests in secs. Default: 30')
    parser.add_argument('--tor-proxies', type=int, default=None,
                        help='Number of Tor proxies to start. Default: 1')
    parser.add_argument('--tor-base-port', type=int, default=13000,
                        help='Smallest port for Tor proxies. Default: 13000')
    parser.add_argument('--tor-timeout', type=int, default=60,
                        help='Timeout in secs for starting Tor process. '
                        'Default: 60')
    parser.add_argument('--tor-retries', type=int, default=3,
                        help='Number of retries for starting Tor process. '
                        'Default: 3')
    args = parser.parse_args()

    if args.crawl_rate is None and args.batch_size is None:
        print('You must specify --crawl-rate or --batch-size.')
        raise SystemExit()
    
    recrawl = None
    if args.recrawl is not None:
        recrawl = parse_datetime(args.recrawl)

    types = None
    if args.types:
        types = args.types.split(',')

    exclude_types = None
    if args.exclude_types:
        exclude_types = args.exclude_types.split(',')

    batch_size = args.batch_size
    if batch_size is None:
        batch_size = int(args.crawl_rate*args.batch_time/args.jobs*1.5)

    logger = Logger()

    # header recommended by shader.io
    headers = {
        'Accept-Encoding' : 'gzip, deflate, br',
        # reed.co.uk seems to be less likely to block windows users
        'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language' : 'en-US,en;q=0.5',
        'Connection' : 'keep-alive',
    }
    request_args = {'headers' : headers}

    proxies = []
    if args.proxies_from is not None:
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

    if not args.tor_proxies:
        crawler = AdzunaCrawler(
            proxies=proxies,
            crawl_rate=args.crawl_rate,
            request_args=request_args,
            request_timeout=args.request_timeout,
            urls_from=args.urls_from,
            recrawl=recrawl,
            types=types,
            exclude_types=exclude_types,
            limit=args.limit,
            max_fail_count=args.max_fail_count,
            jobs=args.jobs,
            batch_size=batch_size,
            batch_time=args.batch_time,
            logger=logger)
    else:
        crawler = AdzunaTorCrawler(
            nproxies=args.tor_proxies,
            crawl_rate=args.crawl_rate,
            request_args=request_args,
            request_timeout=args.request_timeout,
            urls_from=args.urls_from,
            recrawl=recrawl,
            types=types,
            exclude_types=exclude_types,
            limit=args.limit,
            max_fail_count=args.max_fail_count,
            jobs=args.jobs,
            batch_size=batch_size,
            batch_time=args.batch_time,
            tor_base_port=args.tor_base_port,
            tor_timeout=args.tor_timeout,
            tor_retries=args.tor_retries,
            logger=logger)

    crawler.crawl()
