import conf
from crawl import crawl
from logger import Logger
import re
import argparse


directorylink_pattern = re.compile(
    r'(^https?://[a-z]+\.linkedin\.com/directory/)|'
    r'(^https?://[a-z]+\.linkedin\.com/pub/dir/)')


def parse_linkedin(site, url, redirect_url, doc):
    valid = bool(doc.xpath('/html/head/title'))
    if not valid:
        leaf = None,
        links = []
    else:
        leaf = not bool(directorylink_pattern.match(redirect_url))
        linktags = doc.xpath(
            '//*[@id="seo-dir"]/div/div[@class="section last"]/div/ul/li/a')
        links = []
        for tag in linktags:
            link_url = tag.get('href')
            leaf_link = not bool(directorylink_pattern.match(link_url))
            links.append((link_url, leaf_link))

    return valid, leaf, links


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int,
                        help='Maximum number of URLs to crawl.')
    parser.add_argument('--urls-from', 
                        help='Text file holding the URLs to crawl.')
    parser.add_argument('--leafs-only', action='store_true',
                        help='Only crawl leaf pages.')
    parser.add_argument('--recrawl', 
                        help='Recrawl URLs with a timestamp earlier than '
                        'RECRAWL. Format: YYYY-MM-DD')
    parser.add_argument('--max-level', type=int, default=None,
                        help='Maximum click distance from seed websites.')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=500,
                        help='Max. number of URLs to crawl in one batch.')
    parser.add_argument('--batch-time', type=int, default=600,
                        help='Max. time (in secs) to crawl one batch.')
    parser.add_argument('--batch-ips', type=int, default=1,
                        help='Number of IPs per crawl job.')
    parser.add_argument('--base-port', type=int, default=13000,
                        help='Smallest port for Tor proxies.')
    parser.add_argument('--tor-timeout', type=int, default=60,
                        help='Timeout in secs for starting Tor process.')
    parser.add_argument('--tor-retries', type=int, default=3,
                        help='Number of retries for starting Tor process.')
    parser.add_argument('--ip-lifetime', default='120,180',
                        help='Min/Max crawl time in secs separated by a comma. '
                        'Crawl time is the time crawled from the same IP.')    
    parser.add_argument('--delay', default='0,0',
                        help='Min/Max delay between requests in secs '
                        'separated by a comma.')
    parser.add_argument('--request-timeout', type=int, default=30,
                        help='Timeout for requests.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up.')
    args = parser.parse_args()

    ip_lifetime = tuple(float(x) for x in args.ip_lifetime.split(','))
    if len(ip_lifetime) != 2:
        raise RuntimeError(
            'IP lifetime must be a comma separated pair of floats.')
    delay = tuple(float(x) for x in args.delay.split(','))
    if len(delay) != 2:
        raise RuntimeError(
            'Delay must be a comma separated pair of floats.')

    logger = Logger()

    headers = {'Host' : 'uk.linkedin.com'}
    
    crawl('linkedin', parse_linkedin, conf.CRAWL_DB,
          headers=headers,
          urls_from=args.urls_from,
          leafs_only=args.leafs_only,
          recrawl=args.recrawl,
          max_level=args.max_level,
          limit=args.limit,
          max_fail_count=args.max_fail_count,
          jobs=args.jobs,
          batch_size=args.batch_size,
          batch_time=args.batch_time,
          batch_time_tolerance=1,
          batch_ips=args.batch_ips,
          ip_lifetime=ip_lifetime,
          delay=delay,
          request_timeout=args.request_timeout,
          base_port=args.base_port,
          tor_timeout=args.tor_timeout,
          tor_retries=args.tor_retries,
          tor_password=conf.TOR_PASSWORD,
          tor_hashed_password=conf.TOR_HASHED_PASSWORD,
          logger=logger)
