import time
import os
from datetime import datetime, timedelta
import random
import argparse

from lxml import etree
from io import StringIO

from stem import Signal
from stem.control import Controller
import requests

from sqlalchemy import func
import numpy as np

import conf
from crawldb import *
from logger import Logger
from parallelize import ParallelFunction

TOR_COOLDOWN = 11
TIMEOUT = 30
MIN_CRAWL_TIME = 60
MAX_CRAWL_TIME = 120
MIN_DELAY = 0.5
MAX_DELAY = 1.5


def equipartition(l, p):
    if p < 1:
        raise ValueError('Number of partitions must be greater than zero.')
    if p == 1:
        return [l]
    bounds = np.linspace(0, len(l), p+1, dtype=int)
    return [l[lb:ub] for lb, ub in zip(bounds[:-1], bounds[1:])]


def is_valid(site, html):
    html_parser = etree.HTMLParser()
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
    try:
        result = requests.get(url, proxies=proxies, headers=headers,
                              timeout=TIMEOUT)
        success = True
    except Exception as e:
        logger.log('Failed getting URL {0:s}\n{1:s}\nRetrying.\n' \
                   .format(url, str(e)))
    if not success:
        return None
    return result


def crawl_urls(site, urls, deadline, port=9050, control_port=9051):
    logger = Logger()
    with CrawlDB(conf.CRAWL_DB) as crdb:
        last_ipchange = datetime.now()
        crawl_time = random.randint(MIN_CRAWL_TIME, MAX_CRAWL_TIME)
        count = 0
        valid_count = 0
        crawl_start = last_ipchange
        for url in urls:
            timestamp = datetime.now()
            if timestamp > deadline:
                break

            response = get_url(site, url, logger=logger, port=port)
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

            if response is None:
                html = None
                redirect_url = None
                valid = False
            else:
                html = response.text
                redirect_url = response.url
                valid = is_valid(site, html)
            if valid:
                valid_count += 1
            else:
                logger.log('Invalid response for {0:s}\n' \
                           .format(url))

            website = crdb.query(Website) \
                          .filter(Website.site == site,
                                  Website.url == url) \
                          .order_by(Website.timestamp.desc()) \
                          .first()
            if website is None:
                raise IOError('URL {0:s} not found for site {1:s}.' \
                              .format(url, repr(site)))
            if website.valid:
                website = Website(site=site, url=url, fail_count=0)
                crdb.add(website)
            website.html = html
            website.redirect_url = redirect_url
            website.timestamp = timestamp
            website.valid = valid
            if not valid:
                website.fail_count += 1
            crdb.commit()
            count += 1
            logger.log('Crawled {0:d} URLs ({1:d} invalid).\n' \
                       .format(valid_count, count - valid_count))

            if not valid \
               or (timestamp - last_ipchange).total_seconds() > crawl_time:
                last_reload = timestamp
                crawl_time = random.randint(30, 90)
                logger.log('Getting new IP...')
                last_ipchange = new_identity(lastcall=last_ipchange,
                                             port=control_port)
                logger.log('done.\n')
                # ip = get_url('ip', 'http://icanhazip.com/').text.strip()
                # logger.log('Got new IP: {0:s}\n'.format(ip))

        return count, valid_count
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int,
                        help='Maximum number of URLs to crawl.')
    parser.add_argument('--urls-from', 
                        help='Text file holding the URLs to crawl.')
    parser.add_argument('--recrawl', 
                        help='Recrawl URLs with a timestamp earlier than '
                        'RECRAWL. Format: YYYY-MM-DD')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Max. number of URLs to crawl in one batch.')
    parser.add_argument('--batch-time', type=int, default=300,
                        help='Max. time (in secs) to crawl one batch.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up.')
    parser.add_argument('site',
                        help='ID string of the website to crawl.')
    args = parser.parse_args()
    
    crdb = CrawlDB(conf.CRAWL_DB)
    logger = Logger()
    batch_time = timedelta(seconds=args.batch_time)

    if args.recrawl:
        recrawl_date = datetime.strptime(args.recrawl, '%Y-%m-%d')
        website_filter \
            = (~Website.valid) | (Website.timestamp < recrawl_date)
    else:
        website_filter = ~Website.valid
    max_timestamp_col = func.max(Website.timestamp) \
                            .over(partition_by=(Website.site, Website.url)) \
                            .label('max_timestamp')
    q = crdb.query(Website.url, Website.timestamp, max_timestamp_col) \
            .filter(website_filter,
                    Website.site == args.site,
                    Website.fail_count < args.max_fail_count)
    if args.urls_from:
        with open(args.urls_from, 'r') as inputfile:
            urls = [line.strip() for line in inputfile]
        q = q.filter(Website.url.in_(urls))
    q = q.order_by(func.random())
    subq = q.subquery()
    q = crdb.query(subq.c.url) \
            .filter(subq.c.timestamp == subq.c.max_timestamp) \
            .limit(args.batch_size*args.jobs)

    if args.jobs > 1:
        ports = [9060 + p*2 for p in range(args.jobs)]
        control_ports = [9061 + p*2 for p in range(args.jobs)]
    
    count = 0
    valid_count = 0
    crawl_start = datetime.now()
    while True:
        urls = [url for url, in q]
        if not urls:
            break
        if args.limit and len(urls) > args.limit - count:
            urls = urls[:args.limit - count]
            
        deadline = datetime.now() + batch_time
        
        if args.jobs == 1:
            batch_count, batch_valid_count = crawl_urls(
                args.site, urls, deadline)
            count += batch_count
            valid_count += batch_valid_count
        else:
            url_batches = equipartition(urls, args.jobs)
            pargs = [(args.site, url_batch, deadline, port, control_port) \
                     for url_batch, port, control_port in \
                     zip(url_batches, ports, control_ports)]
            pfunc = ParallelFunction(
                crawl_urls, batchsize=1, workdir='crawljobs', prefix='crawl')
            results = pfunc(pargs)
            for batch_count, batch_valid_count in results:
                count += batch_count
                valid_count += batch_valid_count

        logger.log('Crawled {0:d} URLs. Success rate: {1:3.0f}%, '
                   'Crawl rate: {2:5.3f} URLs/sec.\n' \
                   .format(valid_count, valid_count/count*100,
                           valid_count/ \
                           (datetime.now()-crawl_start).total_seconds()))
        
        if args.limit and count >= args.limit:
            break


