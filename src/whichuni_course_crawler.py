from crawler import Crawler
import re

class WhichUniCourseCrawler(Crawler):
    """Crawler class for whichuni courses

    Args:
      site (str, optional): String ID of the crawled webpage. Defaults to
        ``'whichuni'``.
      **kwargs (optional): All other keyword arguments are passed to the
        constructor of ``crawler.Crawler``.

    """

    def __init__(self, site='whichuni', **kwargs):
        Crawler.__init__(self, site, **kwargs)
    
    @classmethod
    def parse(cls, site, url, redirect_url, doc):
        valid = False
        links = []
        page_type = None
        if doc is None:
            return False, None, []
        is_search_page = doc.xpath('//div[@class="search appContainer"]')
        is_course_page = doc.xpath('//div[@class="course appContainer"]')
        if is_search_page or is_course_page:
            valid = True
        if is_search_page:
            page_type = 'search'
            course_linktags = doc.xpath('//a[@class="course-information"]')
            links = [('http://university.which.co.uk' + tag.get('href'), 'course') for tag in course_linktags]
            search_linktags = doc.xpath('//div[@class="pagination"]/a')
            links = links + [('http://university.which.co.uk' + tag.get('href'), 'search') for tag in search_linktags]
        if is_course_page:
            page_type = 'course'
        return valid, page_type, links
        
        