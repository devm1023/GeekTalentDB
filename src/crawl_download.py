import time
import os
from datetime import datetime, timedelta
import random
import argparse

from lxml import etree
from io import StringIO

from tor import new_identity, TorProxyList
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


def get_url(site, url, port=9050, timeout=None, logger=Logger(None)):
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
                              timeout=timeout)
        success = True
    except Exception as e:
        logger.log('Failed getting URL {0:s}\n{1:s}\n' \
                   .format(url, str(e)))
    if not success:
        return None
    return result


def crawl_urls(site, urls, deadline, ports=[9050], control_ports=[9051],
               last_ipchanges=None, crawl_time_range=(120, 180),
               delay_range=(0.5, 1.0), timeout=30):
    logger = Logger()
    nports = len(ports)
    if nports < 1:
        raise ValueError('At least one port is needed.')
    if len(control_ports) != nports:
        raise ValueError('Number of ports must match number of control ports.')
    if last_ipchanges is None:
        now = datetime.now()
        last_ipchanges = [now]*nports
    elif len(last_ipchanges) != nports:
        raise ValueError('Argument last_ipchanges invalid.')
    
    with CrawlDB(conf.CRAWL_DB) as crdb:
        count = 0
        valid_count = 0
        crawl_start = datetime.now()
        for url in urls:
            timestamp = datetime.now()
            if timestamp > deadline:
                break

            iport = random.randint(0, nports-1)
            port = ports[iport]
            control_port = control_ports[iport]
            last_ipchange = last_ipchanges[iport]
            crawl_time = random.uniform(crawl_time_range[0],
                                        crawl_time_range[1])
            
            response = get_url(site, url, logger=logger, port=port,
                               timeout=timeout)
            time.sleep(random.uniform(delay_range[0], delay_range[1]))

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
                logger.log('Getting new IP...')
                last_ipchanges[iport] = datetime.now()
                new_identity(port=control_port, password=conf.TOR_PASSWORD)
                logger.log('done.\n')

        return count, valid_count, last_ipchanges
        

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
    parser.add_argument('--ips-per-job', type=int, default=1,
                        help='Number of identities per crawl job.')
    parser.add_argument('--base-port', type=int, default=13000,
                        help='Smallest port for Tor proxies.')
    parser.add_argument('--tor-timeout', type=int, default=60,
                        help='Timeout in secs for starting Tor process.')
    parser.add_argument('--tor-retries', type=int, default=3,
                        help='Number of retries for starting Tor process.')
    parser.add_argument('--crawl-time-range', default='120,180',
                        help='Min/Max crawl time in secs separated by a comma. '
                        'Crawl time is the time crawled from the same IP.')    
    parser.add_argument('--delay-range', default='0,0',
                        help='Min/Max delay between requests in secs.')
    parser.add_argument('--timeout', type=int, default=30,
                        help='Timeout for requests.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up.')
    parser.add_argument('site',
                        help='ID string of the website to crawl.')
    args = parser.parse_args()

    crawl_time_range = tuple(float(x) for x in args.crawl_time_range.split(','))
    if len(crawl_time_range) != 2:
        raise RuntimeError(
            'Crawl time range must be a comma separated pair of floats.')
    delay_range = tuple(float(x) for x in args.delay_range.split(','))
    if len(delay_range) != 2:
        raise RuntimeError(
            'Delay range must be a comma separated pair of floats.')
    
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
            .filter((subq.c.timestamp == None) | \
                    (subq.c.timestamp == subq.c.max_timestamp)) \
            .limit(args.batch_size*args.jobs)

    with TorProxyList(args.jobs*args.ips_per_job,
                      restart_after=args.tor_timeout,
                      max_restart=args.tor_retries,
                      hashed_password=conf.TOR_HASHED_PASSWORD,
                      logger=logger) as tor_proxies:
        logger.log('Tor proxies started.\n')
        ports = tor_proxies.ports[:]
        control_ports = tor_proxies.control_ports[:]
        last_ipchange_batches = [None]*args.jobs

        tstart = datetime.now()
        total_count = 0
        while True:
            urls = [url for url, in q]
            if not urls:
                break
            if args.limit and len(urls) > args.limit - count:
                urls = urls[:args.limit - count]

            deadline = datetime.now() + batch_time

            if args.jobs == 1:
                count, valid_count = crawl_urls(
                    args.site, urls, deadline, ports, control_ports,
                    crawl_time_range, delay_range, args.timeout)
            else:
                url_batches = equipartition(urls, args.jobs)
                port_batches = equipartition(ports, args.jobs)
                control_port_batches = equipartition(control_ports, args.jobs)
                pargs = []
                for url_batch, port_batch, control_port_batch, \
                    last_ipchange_batch in zip(
                        url_batches, port_batches,
                        control_port_batches, last_ipchange_batches):
                    pargs.append((args.site, url_batch, deadline,
                                  port_batch, control_port_batch,
                                  last_ipchange_batch,
                                  crawl_time_range, delay_range,
                                  args.timeout))
                pfunc = ParallelFunction(crawl_urls, batchsize=1,
                                         workdir='crawljobs', prefix='crawl',
                                         timeout=2*args.batch_time)
                success = False
                try:
                    results = pfunc(pargs)
                    count = 0
                    valid_count = 0
                    last_ipchange_batches = []
                    for batch_count, batch_valid_count, last_ipchange_batch \
                        in results:
                        count += batch_count
                        valid_count += batch_valid_count
                        last_ipchange_batches.append(last_ipchange_batch)
                    success = True
                except TimeoutError:
                    count = 0
                    valid_count = 0
            
            tfinish = datetime.now()
            if not success:
                logger.log('Crawl timed out at {0:s}. Retrying.\n' \
                           .format(tfinish.strftime('%Y-%m-%d %H:%M:%S')))
            else:
                logger.log('Finished batch at {0:s}.\n'
                           'Crawled {1:d} URLs. Success rate: {2:3.0f}%, '
                           'Crawl rate: {3:5.3f} URLs/sec.\n' \
                           .format(tfinish.strftime('%Y-%m-%d %H:%M:%S'),
                                   valid_count, valid_count/count*100,
                                   valid_count/ \
                                   (tfinish-tstart).total_seconds()))
            tstart = tfinish

            total_count += count
            if args.limit and total_count >= args.limit:
                break
