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
        def get_type(url):
            if cls.directory_url_pattern.match(url):
                return 'people-directory'
            elif cls.ukname_url_pattern.match(url):
                return 'name-disambiguation-uk'
            elif cls.name_url_pattern.match(redirect_url):
                return 'name-disambiguation'
            else:
                return 'profile'

        if doc is None:
            return False, get_type(redirect_url), []
            
        valid = False
        type = get_type(redirect_url)
        links = []
        if doc is None:
            return False, type, []
        
        title_elem = doc.xpath('/html/head/title')
        # check for blacklisted page titles
        invalid_titles = ['999: request failed']
        if title_elem and title_elem[0] not in invalid_titles:
            valid = True
        # check if we've been redirected to login page
        if cls.re_login.match(redirect_url):
            valid = False

        if valid:
            if type == 'people-directory':
                linktags = doc.xpath(
                    '//*[@id="seo-dir"]/div/div[@class="section last"]'
                    '/div/ul/li/a')
                links = [(tag.get('href'), get_type(tag.get('href'))) \
                         for tag in linktags]
            elif type == 'name-disambiguation-uk':
                linktags = doc.xpath('//div[@class="profile-card"]/div/h3/a')
                links = [(tag.get('href'), get_type(tag.get('href'))) \
                         for tag in linktags]
            elif type == 'name-disambiguation':
                linktags = doc.xpath('//a[@class="country-specific-link"]')
                links = [(tag.get('href'), get_type(tag.get('href'))) \
                         for tag in linktags]
            else:
                links = []
                if not doc.xpath('//*[@id="name"]'):
                    valid = False

        return valid, type, links
