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

def new_browser():
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
            browser.set_page_load_timeout(10)

            browser.get('https://uk.linkedin.com')
            time.sleep(1)
            letter = chr(random.randint(ord('a'), ord('z')))
            browser.get('https://uk.linkedin.com/directory/people-'+letter)
            time.sleep(1)
            success = True
        except TimeoutException:
            browser.quit()
        
    return browser


def is_valid(body):
    doc = etree.parse(StringIO(body), html_parser)
    return bool(doc.xpath('/html/head/title'))

# with open('lastbody.html', 'r') as inputfile:
#     print(is_valid(inputfile.read()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int,
                        help='Maximum number of profiles to crawl.')
    parser.add_argument('--max-fail-count', type=int, default=10,
                        help='Maximum number of failed crawls before '
                        'giving up.')
    args = parser.parse_args()
    
    crdb = CrawlDB(conf.CRAWL_DB)
    logger = Logger()

    q = crdb.query(LIProfile) \
            .filter(~LIProfile.valid,
                    LIProfile.fail_count < args.max_fail_count) \
            .order_by(func.random())
    if args.limit is not None:
        q = q.limit(args.limit)
    else:
        q = q.limit(10000)
    
    last_reload = datetime.now()
    crawl_time = random.randint(60, 120)
    count = 0
    keep_going = True
    browser = new_browser()
    while keep_going:
        keep_going = False
        for liprofile in q:
            count += 1
            if args.limit is None:
                keep_going = True
            
            success = False
            while not success:
                try:
                    browser.get(liprofile.url)
                    success = True
                except TimeoutException:
                    browser.quit()
                    browser = new_browser()
            time.sleep(random.uniform(0.5, 1.5))
            body = browser.page_source
            redirect_url = browser.current_url
            timestamp = datetime.now()
            valid = is_valid(body)

            liprofile.body = body
            liprofile.redirect_url = redirect_url
            liprofile.timestamp = timestamp
            liprofile.valid = valid
            if not valid:
                liprofile.fail_count += 1
            crdb.commit()
            logger.log('{0:d} profiles crawled.\n'.format(count))

            if not valid \
               or (timestamp - last_reload).total_seconds() > crawl_time:
                last_reload = timestamp
                crawl_time = random.randint(30, 90)
                browser.quit()
                browser = new_browser()

    browser.quit()


