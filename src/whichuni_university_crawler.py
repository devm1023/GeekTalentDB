from crawler import Crawler
import re

class WhichUniUniversityCrawler(Crawler):
    """Crawler class for whichuni categories (fields of study)

    Args:
      site (str, optional): String ID of the crawled webpage. Defaults to
        ``'whichuniuniversities'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    def __init__(self, site='whichuniuniversities', **kwargs):
        Crawler.__init__(self, site, **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        links = []
        page_type = 'university-profile'
        if doc is None:
            return False, page_type, []
        is_subjects = doc.xpath('//div[@class, "institution appContainer"]')
        if is_subjects:
            valid = True
        return valid, page_type, links
        
        