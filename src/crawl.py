__all__ = ['crawl']

import time
import os
from datetime import datetime, timedelta
import random

from lxml import etree
from io import StringIO

from tor import new_identity, TorProxyList
import requests

from sqlalchemy import func
import numpy as np

from crawldb import *
from logger import Logger
from parallelize import ParallelFunction
from pgvalues import in_values


EXCESS = 100
MIN_EXCESS = 10

def equipartition(l, p):
    if p < 1:
        raise ValueError('Number of partitions must be greater than zero.')
    if p == 1:
        return [l]
    bounds = np.linspace(0, len(l), p+1, dtype=int)
    return [l[lb:ub] for lb, ub in zip(bounds[:-1], bounds[1:])]


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


def crawl_urls(site, urls, parsefunc, database, deadline, ports=[9050],
               control_ports=[9051], tor_password='', last_ipchanges=None,
               ip_lifetime=(120, 180), delay_range=(0, 0), timeout=30):
    html_parser = etree.HTMLParser()
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
    
    with CrawlDB(database) as crdb:
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
            crawl_time = random.uniform(ip_lifetime[0],
                                        ip_lifetime[1])
            
            response = get_url(site, url, logger=logger, port=port,
                               timeout=timeout)
            time.sleep(random.uniform(delay_range[0], delay_range[1]))
            if response is None:
                html = None
                redirect_url = None
                valid = False
                leaf = None
            else:
                html = response.text
                parsed_html = etree.parse(StringIO(html), html_parser)
                redirect_url = response.url
                valid, leaf, links = parsefunc(site, url, redirect_url,
                                               parsed_html)
                if not valid:
                    leaf = None
                
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
                website = Website(site=site, url=url, fail_count=0,
                                  level=website.level,
                                  parent_url=website.parent_url)
                crdb.add(website)
            website.html = html
            website.redirect_url = redirect_url
            website.timestamp = timestamp
            website.valid = valid
            website.leaf = leaf
            level = website.level
            if not valid:
                website.fail_count += 1
            crdb.commit()

            if valid and leaf is False:
                for link in links:
                    website = crdb.query(Website) \
                                  .filter(Website.site == site,
                                          Website.url == link) \
                                  .order_by(Website.timestamp.desc()) \
                                  .first()
                    if website is None:
                        website = Website(site=site, url=link, valid=False,
                                          fail_count=0, level=level+1,
                                          parent_url=redirect_url)
                        crdb.add(website)
                    elif website.level > level+1:
                        website.parent_url = redirect_url
                        website.level = level+1
                crdb.commit()
            
            count += 1
            logger.log('Crawled {0:d} URLs ({1:d} invalid).\n' \
                       .format(valid_count, count - valid_count))

            if not valid \
               or (timestamp - last_ipchange).total_seconds() > crawl_time:
                logger.log('Getting new IP...')
                last_ipchanges[iport] = datetime.now()
                new_identity(port=control_port, password=tor_password)
                logger.log('done.\n')

        return count, valid_count, last_ipchanges


def crawl(site, parsefunc, database, urls_from=None, leafs_only=False,
          recrawl=None, max_level=None, limit=None, max_fail_count=10, jobs=1,
          batch_size=500, batch_time=600, batch_time_tolerance=1, batch_ips=1,
          ip_lifetime=(120,180), delay=(0,0), request_timeout=30,
          base_port=13000, tor_timeout=60, tor_retries=3, tor_password='',
          tor_hashed_password='', logger=Logger(None)):

    if not hasattr(ip_lifetime, '__len__'):
        ip_lifetime = (ip_lifetime, ip_lifetime)
    if not hasattr(delay, '__len__'):
        delay = (delay, delay)
            
    crdb = CrawlDB(database)
    batch_time = timedelta(seconds=batch_time)

    if recrawl is not None:
        website_filter \
            = (~Website.valid) | (Website.timestamp < recrawl_date)
    else:
        website_filter = ~Website.valid
    max_timestamp_col = func.max(Website.timestamp) \
                            .over(partition_by=(Website.site, Website.url)) \
                            .label('max_timestamp')
    q = crdb.query(Website.url) \
            .filter(website_filter,
                    Website.site == site,
                    Website.fail_count < max_fail_count)
    if max_level is not None:
        q = q.filter(Website.level <= max_level)
    if urls_from:
        with open(args.urls_from, 'r') as inputfile:
            urls = [line.strip() for line in inputfile]
        q = q.filter(in_values(Website.url, urls))
    if leafs_only:
        q = q.filter(Website.leaf)
    q = q.limit(EXCESS*batch_size*jobs)

    with TorProxyList(jobs*batch_ips,
                      restart_after=tor_timeout,
                      max_restart=tor_retries,
                      hashed_password=tor_hashed_password) as tor_proxies:
        logger.log('Tor proxies started.\n')
        ports = tor_proxies.ports[:]
        control_ports = tor_proxies.control_ports[:]
        last_ipchange_batches = [None]*jobs

        tstart = datetime.now()
        total_count = 0
        while True:
            # get URLs
            urls = set()
            offset = 0
            while len(urls) < MIN_EXCESS*batch_size*jobs:
                new_urls = [url for url, in q.offset(offset)]
                if not new_urls:
                    break
                offset += EXCESS*batch_size*jobs
                urls.update(new_urls)
            urls = list(urls)
            random.shuffle(urls)
            urls = urls[:batch_size*jobs]

            if limit is not None and len(urls) > limit - total_count:
                urls = urls[:args.limit - total_count]

            deadline = datetime.now() + batch_time

            if jobs == 1:
                count, valid_count, last_ipchange_batches[0] = crawl_urls(
                    site, urls, parsefunc, database, deadline, ports,
                    control_ports, tor_password, last_ipchange_batches[0],
                    ip_lifetime, delay, request_timeout)
            else:
                url_batches = equipartition(urls, jobs)
                port_batches = equipartition(ports, jobs)
                control_port_batches = equipartition(control_ports, jobs)
                pargs = []
                for url_batch, port_batch, control_port_batch, \
                    last_ipchange_batch in zip(
                        url_batches, port_batches,
                        control_port_batches, last_ipchange_batches):
                    pargs.append((site, url_batch, parsefunc, database,
                                  deadline, port_batch, control_port_batch,
                                  tor_password, last_ipchange_batch,
                                  ip_lifetime, delay, request_timeout))
                pfunc = ParallelFunction(crawl_urls, batchsize=1,
                                         workdir='crawljobs', prefix='crawljob',
                                         timeout=(1+batch_time_tolerance) \
                                         *batch_time.total_seconds())
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
                logger.log('Crawl timed out at {0:s}. Restarting proxies.\n' \
                           .format(tfinish.strftime('%Y-%m-%d %H:%M:%S')))
                tor_proxies.kill()
                tor_proxies = TorProxyList(jobs*batch_ips,
                                           restart_after=tor_timeout,
                                           max_restart=tor_retries,
                                           hashed_password=tor_hashed_password)
                logger.log('Tor proxies started.\n')
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
            if limit is not None and total_count >= limit:
                break
