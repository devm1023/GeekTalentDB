from crawler import Crawler
import re

class IndeedJobCrawler(Crawler):
    """Crawler class for indeed jobs

    Args:
      site (str, optional): String ID of the crawled webpage. Defaults to
        ``'indeedjob'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    def __init__(self, site='indeedjob', **kwargs):
        Crawler.__init__(self, site, **kwargs)

    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        links = []
        page_type = None
        if doc is None:
            return False, None, []

        job_summary = doc.xpath('//*[@id="job_summary"]')

        valid = False

        if len(job_summary) > 0:
            valid = True

        return valid, page_type, links
        