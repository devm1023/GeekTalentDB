from crawler import Crawler
import re

class WhichUniSubjectCrawler(Crawler):
    """Crawler class for whichuni categories (fields of study)

    Args:
      site (str, optional): String ID of the crawled webpage. Defaults to
        ``'whichunisubjects'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    def __init__(self, site='whichunisubjects', **kwargs):
        Crawler.__init__(self, site, **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        links = []
        page_type = 'alevel-explorer'
        if doc is None:
            return False, page_type, []
        is_subjects = doc.xpath('//li[contains(@class, "result-card")]')
        if is_subjects:
            valid = True
        return valid, page_type, links
        
        