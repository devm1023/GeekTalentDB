import time
import os
from datetime import datetime
import random
import argparse

from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium import webdriver
from selenium.common.exceptions import TimeoutException

from lxml import etree
from io import StringIO

import conf
from crawldb import *
from logger import Logger
from sqlalchemy import func

html_parser = etree.HTMLParser()

def new_browser(site):
    success = False
    while not success:
        try:
            if os.path.exists(conf.TOR_BROWSER_BINARY) is False:
                raise ValueError(
                    'The binary path to Tor firefox does not exist.')
            if os.path.exists(conf.TOR_BROWSER_PROFILE) is False:
                raise ValueError(
                    'The profile path to Tor firefox does not exist.')
            firefox_binary = FirefoxBinary(conf.TOR_BROWSER_BINARY)
            firefox_profile = FirefoxProfile(conf.TOR_BROWSER_PROFILE)
            browser = webdriver.Firefox(firefox_binary=firefox_binary,
                                        firefox_profile=firefox_profile)
            browser.set_page_load_timeout(60)

            enter_site(site, browser)
            success = True
        except TimeoutException:
            browser.quit()
        
    return browser


def enter_site(site, browser):
    if site == 'linkedin':
        browser.get('https://uk.linkedin.com')
        time.sleep(1)
        letter = chr(random.randint(ord('a'), ord('z')))
        browser.get('https://uk.linkedin.com/directory/people-'+letter)
        time.sleep(1)        
    else:
        raise ValueError('Unknown site ID {0:s}'.format(repr(site)))
    

def is_valid(site, html):
    doc = etree.parse(StringIO(html), html_parser)
    if site == 'linkedin':
        return bool(doc.xpath('/html/head/title'))
    else:
        raise ValueError('Unknown site ID {0:s}'.format(repr(site)))


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
    
    last_reload = datetime.now()
    crawl_time = random.randint(60, 120)
    count = 0
    keep_going = True
    browser = new_browser(args.site)
    while keep_going:
        keep_going = False
        for website, max_timestamp in q:
            if website.timestamp != max_timestamp:
                continue
            count += 1
            if args.limit is None:
                keep_going = True
            elif count > args.limit:
                break

            success = False
            while not success:
                try:
                    browser.get(website.url)
                    success = True
                except TimeoutException:
                    browser.quit()
                    browser = new_browser(args.site)
            time.sleep(random.uniform(0.5, 1.5))
            html = browser.page_source
            redirect_url = browser.current_url
            timestamp = datetime.now()
            valid = is_valid(args.site, html)

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
            logger.log('{0:d} profiles crawled.\n'.format(count))

            if not valid \
               or (timestamp - last_reload).total_seconds() > crawl_time:
                last_reload = timestamp
                crawl_time = random.randint(30, 90)
                browser.quit()
                browser = new_browser(args.site)

    browser.quit()


