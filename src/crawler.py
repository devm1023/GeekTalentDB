__all__ = ['Crawler']

import time
import os
from datetime import datetime, timedelta
import random

from lxml import etree
from io import StringIO, BytesIO

from tor import new_identity, TorProxyList
import requests

from sqlalchemy import func
import numpy as np

from crawldb import *
from logger import Logger
from parallelize import ParallelFunction
from pgvalues import in_values

from configurable_object import *
from logger import Logger

# DEBUG
from pprint import pprint
# from collections import namedtuple
# Response = namedtuple('Response', ['url', 'text'])

EXCESS = 100
MIN_EXCESS = 10


def equipartition(l, p):
    if p < 1:
        raise ValueError('Number of partitions must be greater than zero.')
    if p == 1:
        return [l]
    bounds = np.linspace(0, len(l), p+1, dtype=int)
    return [l[lb:ub] for lb, ub in zip(bounds[:-1], bounds[1:])]

def get_url(site, url, proxy=('socks5://127.0.0.1:9050',
                              'socks5://127.0.0.1:9050'),
            request_args={}, timeout=None, logger=Logger(None)):
    request_args = request_args.copy()
    request_args['proxies'] = {'http': proxy[0], 'https': proxy[1]}
    request_args['timeout'] = timeout
    success = False
    try:
        result = requests.get(url, **request_args)
        # result = Response(url=url,
        #                   text='<html><head><title></title></head></html>')
        success = True
    except Exception as e:
        logger.log('Failed getting URL {0:s}\n{1:s}\n' \
                   .format(url, str(e)))
    if not success:
        return None
    return result


def crawl_urls(site, urls, parsefunc, database, deadline, crawl_rate,
               request_args, proxies, timeout, hook, proxy_states):
    html_parser = etree.HTMLParser(encoding='utf-8')
    logger = Logger()
    nproxies = len(proxies)
    if nproxies < 1:
        raise ValueError('At least one port is needed.')
    if crawl_rate is not None:
        mean_request_time = 1/crawl_rate
    else:
        mean_request_time = 0
    if proxy_states is None:
        proxy_states = [None]*nproxies
    
    with CrawlDB(database) as crdb:
        count = 0
        valid_count = 0
        crawl_start = datetime.now()
        discovered_websites = []
        for url in urls:
            timestamp = datetime.now()
            if timestamp > deadline:
                break

            min_request_time = random.uniform(0.5*mean_request_time,
                                              1.5*mean_request_time)
            
            logger.log('{0:s}: Visiting {1:s}\n'.format(str(timestamp), url))
            iproxy = random.randint(0, nproxies-1)
            proxy = proxies[iproxy]

            response = get_url(site, url, proxy=proxy,
                               request_args=request_args,
                               timeout=timeout, logger=logger)
            
            if response is None:
                html = None
                redirect_url = None
                valid = False
                leaf = None
            else:
                html = response.text.encode('utf-8')
                try:
                    parsed_html = etree.parse(BytesIO(html), html_parser)
                except:
                    logger.log('Error parsing HTML:\n')
                    logger.log(response.text)
                    raise
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
                # DEBUG
                logger.log('{0:s} {1:s} {2:s} {3:s}\n' \
                           .format(str(timestamp), str(website.valid),
                                   str(website.timestamp), website.url))
                raise RuntimeError('Recrawling url {0:s}'.format(url))
                website = Website(site=site, url=url, fail_count=0,
                                  level=website.level)
                crdb.add(website)
            website.html = html
            website.redirect_url = redirect_url
            website.timestamp = timestamp
            website.valid = valid
            website.leaf = leaf
            level = website.level
            if not valid:
                website.fail_count += 1
            else:
                website.fail_count = 0
            crdb.commit()

            if valid and leaf is False:
                if level is not None:
                    level += 1
                added_links = set()
                for link_url, link_is_leaf in links:
                    if link_url in added_links:
                        continue
                    else:
                        added_links.add(link_url)
                    discovered_websites.append((link_url, link_is_leaf, level))
                    link = Link(site=site,
                                from_url=redirect_url,
                                to_url=link_url,
                                timestamp=timestamp)
                    crdb.add(link)
                crdb.commit()

            count += 1
            if hook is not None:
                proxy_states[iproxy] \
                    = hook(proxy_states[iproxy], valid, iproxy, proxy)
            
            request_time = (datetime.now() - timestamp).total_seconds()
            if request_time < min_request_time:
                time.sleep(min_request_time - request_time)

    return count, valid_count, proxy_states, discovered_websites


class Crawler(ConfigurableObject):
    """Basic web crawler.
    
    Args:
      site (str): String ID of the site to crawl.
      database (str): URL for the crawl database.
      crawl_rate (float, optional): Desired number of requests/sec. Defaults to
        ``None``, which means crawl as fast as possible.
      proxies (list of tuples of str, optional): List of tuples of the form
        ``(http_proxy, https_proxy)`` specifying proxies to use. Defaults
        to ``[('socks5://127.0.0.1:9050', 'socks5://127.0.0.1:9050')]``.
      request_args (dict, optional): A ``dict`` holding additional keyword
        arguments to ``requests.get``. Defaults to ``{}``.
      request_timeout (float, optional): Timeout in seconds for requests.
        Defaults to 30.
      urls_from (iterable or None, optional): The URLs to crawl. Defaults to
        ``None``, in which case all pages from `database` are crawled
        (subject to constraints defined by the `leafs_only`, `recrawl`,
        `max_level`, `limit`, and `max_fail_count` arguments).
      leafs_only (bool, optional): Crawl only leaf pages. Defaults to ``False``.
      recrawl (datetime or None, optional): Recrawl URLs whose latest timestamp
        is earlier than `recrawl`. Defaults to ``None``, in which case only
        URLs that have not been visited at all are crawled.
      max_level (int or None, optional): Only crawl URLs whose level (click
        distance from the level-0 URLs in `database`) is equal or less than
        `max_level`. Defaults to ``None``, in which case all levels are crawled.
      limit (int or None, optional): Crawl at most `limit` URLs. Defaults to
        ``None``, in which case no limit is applied.
      max_fail_count (int, optional): Only crawl URLs with at most
        `max_fail_count` consecutive fails. Defaults to 10.
      jobs (int, optional): Number of parallel crawl jobs. Defaults to 1.
      batch_size (int, optional): Maximum number of URLs crawled (by one
        parallel job) in one batch. Defaults to 500.
      batch_time (float, optional): Desired time (in seconds) to complete one
        batch. Defaults to 600.
      batch_time_tolerance (float, optional): Controls extra time given to
        parallel jobs before killing them. Parallel jobs will be killed after
        running for ``(1+batch_time_tolerance)*batch_time`` seconds. If this
        happens the ``on_timeout`` method will be called. Defaults to 1.
      workdir (str, optional): Working directory for parallel jobs. Defaults to
        ``'crawljobs'``.
      prefix (str, optional): Prefix for temporary files in `workdir`. Defaults
        to ``'crawljob'``.
      share_proxies (bool, optional): Whether proxies are shared between
        parallel processes. Should be set by derived classes and can only be
        ``True`` for crawlers which use stateless proxies (i.e. when the proxy
        states returned by the ``init_proxies``, ``on_visit``, and
        ``on_timeout`` methods are ``None``). Defaults to ``False``.
      logger (Logger object, optional): Object for writing logging information.
        defaults to ``Logger(None)``, which does not generate any output.

    """
    def check_config(self, config):
        if not isinstance(config['site'], str) or not config['site']:
            raise ValueError('Config option `site` must be non-empy string.')
        if not isinstance(config['database'], str) or not config['database']:
            raise ValueError(
                'Config option `database` must be non-empty string.')
        if not isinstance(config['proxies'], list):
            raise ConfigError("'proxies' option must be a list.")
        if len(config['proxies']) < 1:
            raise ValueError('At least one proxy is needed.')
        if not isinstance(config['jobs'], int) or config['jobs'] < 1:
            raise ValueError('Number of jobs must an int be greater than 0.')
        
    def __init__(self, site, database, **kwargs):
        self.share_proxies = kwargs.pop('share_proxies', False)
        ConfigurableObject.__init__(
            self,
            site=site,
            database=database,
            crawl_rate=None,
            proxies=[('socks5://127.0.0.1:9050', 'socks5://127.0.0.1:9050')],
            request_args={},
            request_timeout=30,
            urls_from=None,
            leafs_only=False,
            recrawl=None,
            max_level=None,
            limit=None,
            max_fail_count=10,
            jobs=1,
            batch_size=500,
            batch_time=600,
            batch_time_tolerance=1,
            workdir = 'crawljobs',
            prefix = 'crawljob',
            logger=Logger(None))
        self.set_config(**kwargs)

    def init_proxies(self, config):
        """User-definable method for initialising proxies.

        Args:
          config (dict): The current config options.

        Returns:
          List of objects describing the proxy states.

        """
        nproxies = len(config['proxies'])
        return [None]*nproxies

    def finish_proxies(self, config, proxy_states):
        """User-definable method for closing proxies.

        Args:
          config (dict): The current config options.
          proxy_states (list): The current states of the proxies.

        """
        pass

    def on_timeout(self, config, proxy_states):
        """User-definable method called after parallel jobs have timed out.

        Args:
          config (dict): The current config options.
          proxy_states (list): The current states of the proxies.

        Returns:
          List of objects describing the new proxy states.

        """
        return proxy_states

    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        """User-definable static method for parsing web pages.

        Args:
          site (str): The string ID of the crawled website.
          url (str): The visited URL.
          redirect_url (str): The URL that the request was redirected to.
          doc (lxml.etree): The lxml etree structure representing the HTML
            code of the web page.

        Returns:
          valid (bool): True if the website was valid.
          leaf (bool): True of the website is a leaf (i.e. contains no links
            that need to be followed).
          links (list): A list of tuples of the form ``(url, is_leaf)`` holding
            the links found on the web page, where `url` is the link URL and
            `is_leaf` is a boolean indicating whether the link target page
            is a leaf page. `is_leaf` may be ``None`` if this can't be
            determined from the URL.

        """
        return True, True, []

    @classmethod
    def on_visit(cls, iproxy, proxy, proxy_state, valid):
        """User-definable static method called after each page visit.

        Args:
          iproxy (int): The index of the proxy that was used.
          proxy (str): The URL of the proxy that was used.
          proxy_state (object): The state object for the used proxy.
          valid (bool): Whether or not a valid page was received.

        Returns:
          object: An object describing the new state of the used proxy.

        """
        pass

    def _check_proxy_states(self, proxy_states, proxies):
        if self.share_proxies:
            proxy_states = [None]*len(proxies)
        if len(proxy_states) != len(proxies):
            raise RuntimeError('List of proxy states and list of proxies '
                               'must have the same length.')
        return proxy_states
        
    
    def crawl(self, **kwargs):
        """Crawl a website.

        Args:
          **kwargs (optional): Accepts the same keyword arguments as the
            constructor and (temporarily) overrides the settings made there.

        """
        config = self.get_config(**kwargs)
        site = config['site']
        parsefunc = self.parse
        database = config['database']
        crawl_rate = config['crawl_rate']
        proxies = config['proxies']
        request_args = config['request_args']
        request_timeout = config['request_timeout']
        urls_from = config['urls_from']
        leafs_only = config['leafs_only']
        recrawl = config['recrawl']
        max_level = config['max_level']
        limit = config['limit']
        max_fail_count = config['max_fail_count']
        jobs = config['jobs']
        batch_size = config['batch_size']
        batch_time = config['batch_time']
        batch_time_tolerance = config['batch_time_tolerance']
        workdir = config['workdir']
        prefix = config['prefix']
        logger = config['logger']

        crdb = CrawlDB(database)
        batch_time = timedelta(seconds=batch_time)
        if crawl_rate is not None:
            crawl_rate = crawl_rate/jobs
        nproxies = len(proxies)

        if recrawl is not None:
            website_filter \
                = (~Website.valid) | (Website.timestamp < recrawl_date)
        else:
            website_filter = ~Website.valid
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
        
        proxy_states = self._check_proxy_states(self.init_proxies(config),
                                                proxies)
        try:
            tstart = datetime.now()            
            total_count = 0
            while True:
                logger.log('Starting batch at {0:s}.\n' \
                           .format(tstart.strftime('%Y-%m-%d %H:%M:%S')))

                # reset session
                crdb.new_session()

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

                if not urls:
                    break
                if limit is not None and len(urls) > limit - total_count:
                    urls = urls[:args.limit - total_count]

                deadline = datetime.now() + batch_time

                # run parallel jobs
                if jobs == 1:
                    success = False
                    try:
                        count, valid_count, proxy_states, \
                            discovered_website_list = crawl_urls(
                                site, urls, parsefunc, database, deadline,
                                crawl_rate, request_args, proxies,
                                request_timeout, self.on_visit, proxy_states)
                        success = True
                    except TimeoutError:
                        count = 0
                        valid_count = 0
                else:
                    url_batches = equipartition(urls, jobs)
                    if self.share_proxies:
                        proxy_batches = [proxies[:] for i in range(jobs)]
                        proxy_state_batches \
                            = [proxy_states[:] for i in range(jobs)]
                    else:
                        proxy_batches = equipartition(proxies, jobs)
                        proxy_state_batches = equipartition(proxy_states, jobs)
                    pargs = []
                    for url_batch, proxy_batch, proxy_state_batch in zip(
                            url_batches, proxy_batches, proxy_state_batches):
                        pargs.append(
                            (site, url_batch, parsefunc, database, deadline,
                             crawl_rate, request_args, proxy_batch,
                             request_timeout, self.on_visit, proxy_state_batch))
                    pfunc = ParallelFunction(crawl_urls, batchsize=1,
                                             workdir=workdir, prefix=prefix,
                                             timeout=(1+batch_time_tolerance) \
                                             *batch_time.total_seconds())
                    success = False
                    try:
                        results = pfunc(pargs)
                        count = 0
                        valid_count = 0
                        proxy_states = []
                        discovered_website_list = []
                        for batch_count, batch_valid_count, proxy_state_batch, \
                            discovered_websites_batch in results:
                            count += batch_count
                            valid_count += batch_valid_count
                            proxy_states.extend(proxy_state_batch)
                            discovered_website_list.extend(
                                discovered_websites_batch)
                        if self.share_proxies:
                            proxy_states = [None]*len(proxies)
                        success = True
                    except TimeoutError:
                        count = 0
                        valid_count = 0
                        proxy_states = None
                        discovered_website_list = []

                # add discovered websites
                discovered_websites = {}
                for url, leaf, level in discovered_website_list:
                    oldleaf, oldlevel \
                        = discovered_websites.get(url, (None, None))
                    if leaf is None:
                        leaf = oldleaf
                    if level is None or \
                       (oldlevel is not None and oldlevel < level):
                        level = oldlevel
                    discovered_websites[url] = (leaf, level)
                for url, (leaf, level) in discovered_websites.items():
                    website = crdb.query(Website) \
                                  .filter(Website.site == site,
                                          Website.url == url) \
                                  .order_by(Website.timestamp.desc()) \
                                  .first()
                    if website is None:
                        website = Website(site=site, url=url, valid=False,
                                          leaf=leaf, fail_count=0, level=level)
                        crdb.add(website)
                    else:
                        if leaf is not None and website.leaf is None:
                            website.leaf = leaf
                        if level is not None and \
                           (website.level is None or website.level > level):
                            website.level = level
                crdb.commit()

                tfinish = datetime.now()
                if not success:
                    logger.log('Crawl timed out at {0:s}.\n' \
                               .format(tfinish.strftime('%Y-%m-%d %H:%M:%S')))
                    proxy_states = self._check_proxy_states(
                        self.on_timeout(config, proxy_states), proxies)
                else:
                    logger.log('Crawled {0:d} URLs. Success rate: {1:3.0f}%, '
                               'Crawl rate: {2:5.3f} URLs/sec.\n' \
                               .format(valid_count, valid_count/count*100,
                                       valid_count/ \
                                       (tfinish-tstart).total_seconds()))
                tstart = tfinish

                total_count += count
                if limit is not None and total_count >= limit:
                    break
        finally:
            self.finish_proxies(config, proxy_states)
        
