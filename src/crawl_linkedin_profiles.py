import sys
sys.path.append('../src')

import time
import os
from datetime import datetime
import random
import argparse

from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium import webdriver

from lxml import etree
from io import StringIO

import conf
from crawldb import *
from logger import Logger
from sqlalchemy import func

html_parser = etree.HTMLParser()

def new_browser():
    binary_path = ('/home/geektalent/.local/opt/tor-browser_en-US/Browser/'
                   'start-tor-browser')
    profile_path = ('/home/geektalent/.local/opt/tor-browser_en-US/Browser/'
                    'TorBrowser/Data/Browser/profile.default/')
    if os.path.exists(binary_path) is False:
        raise ValueError("The binary path to Tor firefox does not exist.")
    if os.path.exists(profile_path) is False:
        raise ValueError("The profile path to Tor firefox does not exist.")
    firefox_binary = FirefoxBinary(binary_path)
    firefox_profile = FirefoxProfile(profile_path)
    browser = webdriver.Firefox(firefox_binary=firefox_binary,
                                firefox_profile=firefox_profile)
    browser.get('https://uk.linkedin.com')
    time.sleep(1)
    letter = chr(random.randint(ord('a'), ord('z')))
    browser.get('https://uk.linkedin.com/directory/people-'+letter)
    time.sleep(1)
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
    
    last_reload = datetime.now()
    crawl_time = random.randint(60, 120)
    count = 0
    browser = new_browser()
    for liprofile in q:
        count += 1
        browser.get(liprofile.url)
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
        
        if not valid or (timestamp - last_reload).total_seconds() > crawl_time:
            last_reload = timestamp
            crawl_time = random.randint(30, 90)
            browser.quit()
            browser = new_browser()

    browser.quit()


