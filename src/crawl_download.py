import time
import os
from datetime import datetime
import random
import argparse

from lxml import etree
from io import StringIO

from stem import Signal
from stem.control import Controller
import requests

import conf
from crawldb import *
from logger import Logger
from sqlalchemy import func

html_parser = etree.HTMLParser()
TOR_COOLDOWN = 11
TIMEOUT = 30


def is_valid(site, html):
    doc = etree.parse(StringIO(html), html_parser)
    if site == 'linkedin':
        return bool(doc.xpath('/html/head/title'))
    else:
        raise ValueError('Unknown site ID {0:s}'.format(repr(site)))


def new_identity(lastcall=None, port=9051):
    now = datetime.now()
    if lastcall is not None:
        secs_since_last_call = (now-lastcall).total_seconds()
        if secs_since_last_call < TOR_COOLDOWN:
            time.sleep(TOR_COOLDOWN - secs_since_last_call)
    with Controller.from_port(port=port) as controller:
        controller.authenticate(password='PythonRulez')
        controller.signal(Signal.NEWNYM)
    return datetime.now()


def get_url(site, url, port=9050, logger=Logger(None)):
    headers = {
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding' : 'gzip, deflate, sdch',
        'Accept-Language' : 'en-US,en;q=0.8,de;q=0.6',
        'Connection' : 'keep-alive',
        'DNT' : '1',
        'Host' : 'uk.linkedin.com',
        'Upgrade-Insecure-Requests' : '1',
        'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
    }
    proxies = {'http':  'socks5://127.0.0.1:{0:d}'.format(port),
               'https': 'socks5://127.0.0.1:{0:d}'.format(port)}
    success = False
    while not success:
        try:
            result = requests.get(url, proxies=proxies, headers=headers,
                                  timeout=TIMEOUT)
            success = True
        except Exception as e:
            logger.log('Failed getting URL {0:s}\n{1:s}\nRetrying.\n' \
                       .format(url, str(e)))
            time.sleep(2)
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int,
                        help='Maximum number of profiles to crawl.')
    parser.add_argument('--urls-from', 
                        help='Text file holding the URLs to crawl.')
    parser.add_argument('--recrawl', 
                        help='Recrawl profiles with a timestamp earlier than '
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up.')
    parser.add_argument('site',
                        help='ID string of the website to crawl.')
    args = parser.parse_args()
    
    crdb = CrawlDB(conf.CRAWL_DB)
    logger = Logger()

    if args.recrawl:
        recrawl_date = datetime.strptime(args.recrawl, '%Y-%m-%d')
        profile_filter \
            = (~Website.valid) | (Website.timestamp < recrawl_date)
    else:
        profile_filter = ~Website.valid
    max_timestamp_col = func.max(Website.timestamp) \
                            .over(partition_by=Website.url) \
                            .label('max_timestamp')
    q = crdb.query(Website, max_timestamp_col) \
            .filter(profile_filter,
                    Website.site == args.site,
                    Website.fail_count < args.max_fail_count)
    if args.urls_from:
        with open(args.urls_from, 'r') as inputfile:
            urls = [line.strip() for line in inputfile]
        q = q.filter(Website.url.in_(urls))
    q = q.order_by(func.random()).limit(10000)
    
    last_ipchange = datetime.now()
    crawl_time = random.randint(60, 120)
    count = 0
    valid_count = 0
    crawl_start = datetime.now()
    keep_going = True
    while keep_going:
        keep_going = False
        for website, max_timestamp in q:
            if website.timestamp != max_timestamp:
                continue
            count += 1
            if args.limit is None or count < args.limit:
                keep_going = True
            if args.limit is not None and count > args.limit:
                break

            response = get_url(args.site, website.url, logger=logger)
            time.sleep(random.uniform(0.5, 1.5))
            
            html = response.text
            redirect_url = response.url
            timestamp = datetime.now()
            valid = is_valid(args.site, html)
            if valid:
                valid_count += 1
            else:
                logger.log('Got invalid response for URL {0:s}\n' \
                           .format(website.url))

            if website.valid:
                website = Website(fail_count=0)
                crdb.add(website)
            website.html = html
            website.redirect_url = redirect_url
            website.timestamp = timestamp
            website.valid = valid
            if not valid:
                website.fail_count += 1
            crdb.commit()
            logger.log('Crawled {0:d} profiles. Success rate: {1:3.0f}%, '
                       'Crawl rate: {2:5.3f} prf/sec.\n' \
                       .format(valid_count, valid_count/count*100,
                               valid_count/ \
                               (timestamp-crawl_start).total_seconds()))

            if not valid \
               or (timestamp - last_ipchange).total_seconds() > crawl_time:
                last_reload = timestamp
                crawl_time = random.randint(30, 90)
                last_ipchange = new_identity(lastcall=last_ipchange)
                ip = get_url('ip', 'http://icanhazip.com/').text.strip()
                logger.log('Got new IP: {0:s}\n'.format(ip))
                

