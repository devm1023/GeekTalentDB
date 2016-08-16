"""Crawl websites and store the HTML in the `crawl` database.

This module provides the ``Crawler`` class which serves as a base class for
objects that crawl specific websites and store the HTML in the `crawl` database.

Created by: Martin Wiebusch
Last modified: 2016-07-27 MW

"""

__all__ = ['Crawler']

import time
import os
from datetime import datetime, timedelta
import random

from htmlextract import parse_html

from tor import new_identity, TorProxyList
import requests

from sqlalchemy import func
import numpy as np

from configurable_object import *
from crawldb import *
from logger import Logger
from parallelize import ParallelFunction
from pgvalues import in_values
from windowquery import split_process, collapse


EXCESS = 100
MIN_EXCESS = 10
EPOCH = datetime(1970, 1, 1)


class CrawlDBCheckError(RuntimeError):
    pass


def equipartition(l, p):
    """Partition list `l` into `p` (approximately) even-sized chunks.

    """
    if p < 1:
        raise ValueError('Number of partitions must be greater than zero.')
    if p == 1:
        return [l]
    bounds = np.linspace(0, len(l), p+1, dtype=int)
    return [l[lb:ub] for lb, ub in zip(bounds[:-1], bounds[1:])]


def time_to_microsec(t):
    """Convert time `t` to an integer timestamp at microsecond precision.

    Returns the number of microseconds after Jan 1, 1970.

    """
    return int((t - EPOCH).total_seconds()*1e6)


def get_url(site, url, proxy=('socks5://127.0.0.1:9050',
                              'socks5://127.0.0.1:9050'),
            request_args={}, timeout=None, logger=Logger(None)):
    """Attempt to retreive URL through a given proxy.

    Note:
      This function catches all exceptions from ``requests.get``, logs them and
      returns ``None`` in the case of a failed request.

    Args:
      site (str): String identifying the crawled website. Currently not used.
      url (str): The URL to visit.
      proxy (tuple of str, optional): Tuple holding the URLs of the http and
        https proxies (in that order). Default settings assume a local Tor
        proxy.
      request_args (dict, optional): Keyword arguments for the call to
        ``requests.get``. Defaults to ``{}``.
      timeout (float or None, optional): Timeout for the request in secs.
        Defaults to ``None``, in which case no timeout is set.
      logger (Logger object, optional): Object for writing log messages.

    Returns:
      A valid response object or ``None`` in case of a failed request.

    """
    request_args = request_args.copy()
    request_args['proxies'] = {'http': proxy[0], 'https': proxy[1]}
    request_args['timeout'] = timeout
    success = False

    try:
        result = requests.get(url, **request_args)
        if result.status_code < 200 or result.status_code > 399:
            raise RuntimeError('Received status code {0:d}.' \
                               .format(result.status_code))
        success = True
    except Exception as e:
        logger.log('Failed getting URL {0:s}\n{1:s}\n' \
                   .format(url, str(e)))
    if not success:
        return None
    return result


def _in_range(ts, from_ts, to_ts):
    if from_ts is not None and ts < from_ts:
        return False
    if to_ts is not None and ts >= to_ts:
        return False
    return True


def make_website(id, site, url, redirect_url, timestamp, html, expected_type,
                 parsefunc, require_valid_html=False, logger=Logger(None)):
    if timestamp is None:
        return dict(
            id=None,
            site=site,
            url=url,
            redirect_url=None,
            timestamp=None,
            html=None,
            type=expected_type,
            valid=False,
            fail_count=0,
            links=[]
        )

    parsed_html = None
    if html is not None:
        try:
            parsed_html = parse_html(html)
        except:
            if id is not None:
                msg = 'Failed parsing HTML for ID {0:d}'.format(id)
            else:
                msg = 'Failed parsing HTML for URL {0:s}'.format(url)

            if require_valid_html:
                raise CrawlDBCheckError(msg)
            else:
                logger.log(msg+'.\n')
                filename = 'parsefail-{0:d}.html' \
                           .format(time_to_microsec(timestamp))
                with open(filename, 'w') as htmlfile:
                    htmlfile.write(html)

    valid, type, links = parsefunc(site, url, redirect_url, parsed_html)
    if parsed_html is None:
        valid = False
        links = []

    if not valid:
        type = expected_type
    
    if not isinstance(valid, bool):
        raise RuntimeError(
            'Parse function must return boolean value for `valid`')
    if not isinstance(type, str) and type is not None:
        raise RuntimeError(
            'Parse function must return None or string for `type`')
    if not isinstance(links, list):
        raise RuntimeError(
            'Parse function must return list value for `links`')
    if expected_type is not None and type != expected_type:
        raise RuntimeError(
            'Website type does not match expected type')

    website = dict(
        id=None,
        site=site,
        url=url,
        redirect_url=redirect_url,
        timestamp=timestamp,
        fail_count=1 if not valid and timestamp is not None else 0,
        html=html,
        type=type,
        valid=valid,
        links=[]
    )

    added_links = {}
    for link_url, link_type in links:
        if not isinstance(link_url, str):
            raise RuntimeError(
                'Parse function must return str value for link URL')
        link_url = link_url.strip()
        if not link_url:
            raise RuntimeError(
                'Parse function must return non-empty string for link URL')
        if not isinstance(link_type, str) and link_type is not None:
            raise RuntimeError(
                'Parse function must return None or string for link type')

        if link_url in added_links:
            if link_type == added_links[link_url]:
                continue
            else:
                raise RuntimeError(
                    'Inconsistent link type for link URL {0:s}' \
                    .format(link_url))
        link = dict(
            url=link_url,
            type=link_type
        )
        website['links'].append(link)
        added_links[link_url] = link_type

    return website


def check_urls(jobid, from_url, to_url, site, parsefunc,
               repair, from_ts, to_ts):
    logger = Logger()
    with CrawlDB() as crdb:
        q = crdb.query(Website.url, Website) \
                .filter(Website.site == site,
                        Website.url >= from_url)
        if to_url is not None:
            q = q.filter(Website.url < to_url)
        q = q.order_by(Website.url, Website.timestamp)

        for url, websites in collapse(q):
            websites = [w for w, in websites]

            # check consistency of fields
            if len(websites) > 1:
                if any(w.timestamp is None for w in websites):
                    raise CrawlDBCheckError(
                        'Found multiple crawls and a missing timestamp for '
                        'URL {0:s}'.format(url))
                w = websites[-1]
                if not w.valid and w.fail_count <= 0:
                    raise CrawlDBCheckError(
                        'Multiple crawls and failed last crawl with '
                        'non-positive fail count for ID {0:d}'.format(w.id))
            for w in websites[:-1]:
                if not w.valid:
                    raise CrawlDBCheckError(
                        'Failed crawl which is not last crawl for '
                        'ID {0:d}'.format(w.id))
            for w in websites:
                if w.redirect_url is None and w.html is not None:
                    raise CrawlDBCheckError(
                        'Missing redirect URL for ID {0:d}'.format(w.id))
                if w.timestamp is None and w.valid:
                    raise CrawlDBCheckError(
                        'Valid crawl with missing timestamp for '
                        'ID {0:d}'.format(w.id))
                if w.valid and w.fail_count != 0:
                    msg = ('Valid crawl with non-zero fail count for '
                           'ID {0:d}'.format(w.id))
                    if repair and _in_range(w.timestamp, from_ts, to_ts):
                        logger.log('{0:s}. Repairing.\n'.format(msg))
                        w.fail_count = 0
                    else:
                        raise CrawlDBCheckError(msg)

            # check validity of websites
            for w in websites:
                if not _in_range(w.timestamp, from_ts, to_ts):
                    continue
                require_valid_html = w.valid and not repair
                needs_repair = False
                wdict = make_website(
                    w.id, w.site, w.url, w.redirect_url, w.timestamp, w.html,
                    w.type, parsefunc, require_valid_html=require_valid_html,
                    logger=logger)
                wdict['id'] = w.id
                if w.timestamp and not wdict['valid'] and not w.valid:
                    wdict['fail_count'] = w.fail_count

                if w.valid != wdict['valid']:
                    msg = ('Wrong `valid` field for ID {0:d}. '
                           'Is {1:s}, should be {2:s}.') \
                           .format(w.id, str(w.valid), str(wdict['valid']))
                    if repair:
                        logger.log(msg+' Repairing.\n')
                        needs_repair = True
                    else:
                        raise CrawlDBCheckError(msg)
                if w.fail_count != wdict['fail_count']:
                    msg = ('Wrong `fail_count` field for ID {0:d}. '
                           'Is {1:d}, should be {2:d}.') \
                           .format(w.id, w.fail_count, wdict['fail_count'])
                    if repair:
                        logger.log(msg+' Repairing.\n')
                        needs_repair = True
                    else:
                        raise CrawlDBCheckError(msg)
                if w.type != wdict['type']:
                    msg = ('Wrong `type` field for ID {0:d}. '
                           'Is {1:s}, should be {2:s}.') \
                           .format(w.id, repr(w.type), repr(wdict['type']))
                    if repair:
                        logger.log(msg+' Repairing.\n')
                        needs_repair = True
                    else:
                        raise CrawlDBCheckError(msg)
                old_links = set((l.url, l.type) for l in w.links)
                new_links = set((l['url'], l['type']) \
                                for l in wdict['links'])
                for link in old_links:
                    if link not in new_links:
                        msg = 'Spurious link {0:s} for ID {1:d}.' \
                              .format(str(link), w.id)
                    if repair:
                        logger.log(msg+' Repairing.\n')
                        needs_repair = True
                    else:
                        raise CrawlDBCheckError(msg)
                for link in new_links:
                    if link not in old_links:
                        msg = 'Missing link {0:s} for ID {1:d}.' \
                              .format(str(link), w.id)
                    if repair:
                        logger.log(msg+' Repairing.\n')
                        needs_repair = True
                    else:
                        raise CrawlDBCheckError(msg)

                if needs_repair:
                    crdb.add_from_dict(wdict, Website)
                    crdb.flush()

            # check conditions for multiple crawls again
            if len(websites) > 1:
               if any(w.timestamp is None for w in websites):
                   raise RuntimeError(
                       'Found multiple crawls and a missing timestamp for '
                       'URL {0:s} after repair'.format(url))
               if any(w.valid is None for w in websites):
                   raise RuntimeError(
                       'Found multiple crawls and valid = null for '
                       'URL {0:s} after repair'.format(url))

            # combine subsequent failed crawls
            if websites and repair:
                lastw = websites[0]
                for w in websites[1:]:
                    if not lastw.valid:
                        if not w.valid:
                            w.fail_count += lastw.fail_count
                        crdb.delete(lastw)
                    lastw = w

            crdb.commit()


def crawl_urls(site, urls, parsefunc, deadline, crawl_rate,
               request_args, proxies, timeout, hook, proxy_states):
    """Sequentially crawl a list of URLs and store the HTML.

    Note:
      For each request this function randomly selects a proxy from a supplied
      list of proxies. If required, state information about the proxies can
      be supplied via the `proxy_states` argument and managed via the `hook`
      argument.

    Args:
      site (str): String identifying the website.
      urls (list of str): List of URLs to visit.
      parsefunc (callable): Function for validating the HTML and extracting
        links. See documentation of ``Crawler.parse``.
      deadline (datetime object): Time by which the function should terminate.
        URLs which were not crawled by `deadline` will be skipped.
      crawl_rate (float): Desired crawl rate in requests per second.
      request_args (dict): Keyword arguments for ``requests.get``.
      proxies (list of tuples of str): List of proxies to (randomly) choose
        from. Each tuple holds the URLs of the http and https proxy (in that
        order).
      timeout (float): Timeout for requests in secs.
      hook (callable or None): Hook for updating proxy states. Called after
        each request. See ``Crawler.on_visit`` for function signature.
      proxy_states (list of object): Objects describing the states of the
        proxies.

    """
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
    
    with CrawlDB() as crdb:
        count = 0
        valid_count = 0
        crawl_start = datetime.utcnow()
        discovered_websites = []
        for url in urls:
            timestamp = datetime.utcnow()
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
            
            html = None
            redirect_url = None
            valid = False
            if response is not None:
                html = response.text
                redirect_url=response.url

            # retrieve last crawl from database
            website = crdb.query(Website) \
                          .filter(Website.site == site,
                                  Website.url == url) \
                          .order_by(Website.timestamp.desc()) \
                          .first()
            if website is None:
                raise IOError('URL {0:s} not found for site {1:s}.' \
                              .format(url, repr(site)))

            # Create a new row if the last visit to this site was successful.
            # Update the row if this visit was successful and this is the first
            # visit or the last one was unsuccessful.
            # Increment fail_count if this and the last visit were unsuccessful.
            website_dict = make_website(
                None, site, url, redirect_url, timestamp, html, website.type,
                parsefunc, require_valid_html=False, logger=logger)
            if website.valid is not True:
                website_dict['id'] = website.id
                if not website_dict['valid']:
                    website_dict['fail_count'] += website.fail_count
            crdb.add_from_dict(website_dict, Website)
            crdb.commit()

            count += 1
            # Update proxy state.
            if hook is not None:
                proxy_states[iproxy] \
                    = hook(proxy_states[iproxy], valid, iproxy, proxy)

            # Throttle crawl to desired crawl rate
            request_time = (datetime.utcnow() - timestamp).total_seconds()
            if request_time < min_request_time:
                time.sleep(min_request_time - request_time)

    return count, valid_count, proxy_states


class Crawler(ConfigurableObject):
    """Basic web crawler.
    
    Args:
      site (str): String ID of the site to crawl.
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
        ``None``, in which case all pages from the database are crawled
        (subject to constraints defined by the `recrawl`, `limit`, 
        `max_fail_count`, `types` and `exclude_types` arguments).
      recrawl (datetime or None, optional): Recrawl URLs whose latest timestamp
        is earlier than `recrawl`. Defaults to ``None``, in which case only
        URLs that have not been visited at all are crawled.
      types (list of str or None, optional): Only crawl URLs whose type one of
        those listed in `types`. Defaults to ``None``, in which case all types
        of URLs are crawled.
      exclude_types (list of str, optional): Exclude URLs of the specified types
        from crawl. Defaults to ``[]``.
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
        if not isinstance(config['proxies'], list):
            raise ConfigError("'proxies' option must be a list.")
        if len(config['proxies']) < 1:
            raise ValueError('At least one proxy is needed.')
        if not isinstance(config['jobs'], int) or config['jobs'] < 1:
            raise ValueError('Number of jobs must an int be greater than 0.')
        
    def __init__(self, site, **kwargs):
        self.share_proxies = kwargs.pop('share_proxies', False)
        ConfigurableObject.__init__(
            self,
            site=site,
            crawl_rate=None,
            proxies=[('socks5://127.0.0.1:9050', 'socks5://127.0.0.1:9050')],
            request_args={},
            request_timeout=30,
            urls_from=None,
            recrawl=None,
            types=None,
            exclude_types=[],
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
          links (list or str): A list of the link URLs found on the web page.

        """
        return True, None, []

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
        crawl_rate = config['crawl_rate']
        proxies = config['proxies']
        request_args = config['request_args']
        request_timeout = config['request_timeout']
        urls_from = config['urls_from']
        recrawl = config['recrawl']
        types = config['types']
        exclude_types = config['exclude_types']
        limit = config['limit']
        max_fail_count = config['max_fail_count']
        jobs = config['jobs']
        batch_size = config['batch_size']
        batch_time = config['batch_time']
        batch_time_tolerance = config['batch_time_tolerance']
        workdir = config['workdir']
        prefix = config['prefix']
        logger = config['logger']

        with CrawlDB() as crdb:
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
                            Website.fail_count <= max_fail_count)
            if types:
                q = q.filter(Website.type.in_(types))
            if exclude_types:
                q = q.filter(~Website.type.in_(exclude_types))
            if urls_from:
                with open(args.urls_from, 'r') as inputfile:
                    urls = [line.strip() for line in inputfile]
                q = q.filter(in_values(Website.url, urls))
            q = q.limit(EXCESS*batch_size*jobs)

            proxy_states = self._check_proxy_states(self.init_proxies(config),
                                                    proxies)
            try:
                tstart = datetime.utcnow()            
                total_count = 0
                while True:
                    logger.log('Starting batch at {0:s}.\n' \
                               .format(tstart.strftime('%Y-%m-%d %H:%M:%S')))

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

                    deadline = datetime.utcnow() + batch_time

                    # run parallel jobs
                    if jobs == 1:
                        success = False
                        try:
                            count, valid_count, proxy_states = crawl_urls(
                                site, urls, parsefunc, deadline,
                                crawl_rate, request_args, proxies,
                                request_timeout, self.on_visit,
                                proxy_states)
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
                            proxy_state_batches = equipartition(proxy_states,
                                                                jobs)
                        pargs = []
                        for url_batch, proxy_batch, proxy_state_batch \
                            in zip(url_batches, proxy_batches,
                                   proxy_state_batches):
                            pargs.append(
                                (site, url_batch, parsefunc, deadline,
                                 crawl_rate, request_args, proxy_batch,
                                 request_timeout, self.on_visit,
                                 proxy_state_batch))
                        pfunc = ParallelFunction(
                            crawl_urls, batchsize=1, workdir=workdir,
                            prefix=prefix, timeout=(1+batch_time_tolerance) \
                            *batch_time.total_seconds())
                        success = False
                        try:
                            results = pfunc(pargs)
                            count = 0
                            valid_count = 0
                            proxy_states = []
                            for batch_count, batch_valid_count, \
                                proxy_state_batch in results:
                                
                                count += batch_count
                                valid_count += batch_valid_count
                                proxy_states.extend(proxy_state_batch)
                            if self.share_proxies:
                                proxy_states = [None]*len(proxies)
                            success = True
                        except TimeoutError:
                            count = 0
                            valid_count = 0
                            proxy_states = None

                    tfinish = datetime.utcnow()
                    if not success:
                        logger.log(
                            'Crawl timed out at {0:s}.\n' \
                            .format(tfinish.strftime('%Y-%m-%d %H:%M:%S')))
                        proxy_states = self._check_proxy_states(
                            self.on_timeout(config, proxy_states), proxies)
                    else:
                        logger.log(
                            'Crawled {0:d} URLs. Success rate: {1:3.0f}%, '
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
        

    def check_db(self, repair=False, from_timestamp=None, to_timestamp=None,
                 **kwargs):
        config = self.get_config(**kwargs)
        site = config['site']
        parsefunc = self.parse
        jobs = config['jobs']
        batch_size = config['batch_size']
        workdir = config['workdir']
        prefix = config['prefix']
        logger = config['logger']

        with CrawlDB() as crdb:
            q = crdb.query(Website.url) \
                    .filter(Website.site == site)

            split_process(q, check_urls, batch_size, njobs=jobs,
                          args=[site, self.parse,
                                repair, from_timestamp, to_timestamp],
                          logger=logger, workdir=workdir, prefix=prefix)

