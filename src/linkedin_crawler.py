from crawler import Crawler
import re


class LinkedInCrawler(Crawler):
    """Crawler class for LinkedIn profiles and the people directory.

    Args:
      site (str, optional): String ID of the crawled website. Defaults to
        ``'linkedin'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    # static regexes for matching different types of URLs
    directory_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/directory/')
    name_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/')
    ukname_url_pattern = re.compile(
        r'^https?://uk\.linkedin\.com/pub/dir/.+/gb-0-United-Kingdom')
    re_login = re.compile(r'^https?://(www|[a-z][a-z])\.linkedin\.com(/hp)?/?$')

    def __init__(self, site='linkedin', **kwargs):
        if 'request_args' not in kwargs:
            kwargs['request_args'] = {}
        if 'headers' not in kwargs['request_args']:
            kwargs['request_args']['headers'] = {
                'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding' : 'gzip, deflate, sdch',
                'Accept-Language' : 'en-US,en;q=0.8,de;q=0.6',
                'Connection' : 'keep-alive',
                'DNT' : '1',
                'Host' : 'uk.linkedin.com',
                'Upgrade-Insecure-Requests' : '1',
                'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
            }
        kwargs.pop('share_proxies', None)
        Crawler.__init__(self, site, share_proxies=True, **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        title_elem = doc.xpath('/html/head/title')
        # check for blacklisted page titles
        invalid_titles = ['999: request failed']
        if title_elem and title_elem[0] not in invalid_titles:
            valid = True
        # check if we've been redirected to login page
        if cls.re_login.match(redirect_url):
            valid = False
            
        if not valid:
            links = []
        elif cls.directory_url_pattern.match(redirect_url):
            # We are crawling the people directory
            linktags = doc.xpath(
                '//*[@id="seo-dir"]/div/div[@class="section last"]/div/ul/li/a')
            links = [(tag.get('href'), False) for tag in linktags]
        elif cls.ukname_url_pattern.match(redirect_url):
            # We're on a name disambiguation page restricted to UK profiles
            linktags = doc.xpath('//div[@class="profile-card"]/div/h3/a')
            links = [(tag.get('href'), True) for tag in linktags]
        elif cls.name_url_pattern.match(redirect_url):
            # We're on an international name disambiguation page
            linktags = doc.xpath('//a[@class="country-specific-link"]')
            links = [(tag.get('href'), False) for tag in linktags]
        else:
            # We're on a profile page
            links = []
            # Check if the name field exists
            if not doc.xpath('//*[@id="name"]'):
                valid = False

        return valid, links
