"""Crawl public LinkedIn profiles.

"""

from linkedin_crawler import Crawler
from parse_datetime import parse_datetime
from logger import Logger
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--proxies-from', default=None,
                        help='Load proxy URLs from text file. '
                        'Default proxy: ')
    parser.add_argument('--max-level', type=int, default=None,
                        help='Maximum click distance from seed websites.')
    parser.add_argument('--recrawl', 
                        help='Recrawl URLs with a timestamp earlier than '
                        'RECRAWL.')
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
        recrawl = parse_datetime(args.recrawl)
    
    logger = Logger()

    # header recommended by shader.io
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
    
