from crawler import Crawler
import re
from lxml import html
import requests


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

        cleantext = ''

        if len(landing_link) > 0:
            # adzuna landing page
            page_type = 'redirect'

            links = [(landing_link[0].get('href'), '')]
            print("#################################")
            landing_link = landing_link[0].get('href')
            print(landing_link)
            page = requests.get(landing_link, verify=False, timeout=10)
            doc = html.fromstring(page.content)
            cv_library_job = doc.xpath('//div[@class="jd-details jobview-desc"]')
            railway_job = doc.xpath('//div[@class="job-description clearfix"]')
            technojob = doc.xpath('//div[@class="job-listing-body"]')
            telegraph_job = doc.xpath('//div[@class="block fix-text job-description"]')
            aplitrak_job = doc.xpath('//div[@class="description"]')
            charity_job = doc.xpath('//div[@class="description-body"]')
            jobserve = doc.xpath('//div[@class="searchDescription"]')
            print("############################")

            if len(cv_library_job) > 0:
                print("===============================")
                print(cv_library_job)
                raw_html = html.tostring(cv_library_job[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(railway_job) > 0:
                print("===============================")
                print(railway_job)
                raw_html = html.tostring(railway_job[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(technojob) > 0:
                print("===============================")
                print(technojob)
                raw_html = html.tostring(technojob[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(telegraph_job) > 0:
                print("===============================")
                print(telegraph_job)
                raw_html = html.tostring(telegraph_job[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(aplitrak_job) > 0:
                print("===============================")
                print(aplitrak_job)
                raw_html = html.tostring(aplitrak_job[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(charity_job) > 0:
                print("===============================")
                print(charity_job)
                raw_html = html.tostring(charity_job[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")
            if len(jobserve) > 0:
                print("===============================")
                print(jobserve)
                raw_html = html.tostring(jobserve[0], pretty_print=True).decode('utf-8')
                cleanr = re.compile('<.*?>')
                cleantext = re.sub(cleanr, '', raw_html)
                print("===============================")

        elif len(adzuna_job_details) > 0:
            page_type = 'post'
            print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
            print(adzuna_job_details)
            raw_html = html.tostring(adzuna_job_details[0], pretty_print=True).decode('utf-8')
            cleanr = re.compile('<.*?>')
            cleantext = re.sub(cleanr, '', raw_html)
            print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")

        else:
            page_type = 'external-post'

        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        print(cleantext)
        print(type(cleantext))
        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

        # TODO: Get full description

        # fill_description = cleantext

        return valid, page_type, links, cleantext