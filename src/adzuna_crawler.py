from crawler import Crawler
import re

class AdzunaCrawler(Crawler):
    """Crawler class for adzuna jobs

    Args:
      site (str, optional): String ID of the crawled webpage. Defaults to
        ``'adzuna'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    def __init__(self, site='adzuna', **kwargs):
        Crawler.__init__(self, site, **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        links = []
        page_type = None
        if doc is None:
            return False, None, []


        landing_link = doc.xpath('//a[.="view ad here"]')
        adzuna_job_details = doc.xpath('//div[@class="ad_details_contents"]')
        
        valid = True

        if len(landing_link) > 0:
            # adzuna landing page
            page_type = 'redirect'
            links = [(landing_link[0].get('href'), '')]
        elif len(adzuna_job_details) > 0:
            page_type = 'post'
        else:
            page_type = 'external-post'

        return valid, page_type, links
        