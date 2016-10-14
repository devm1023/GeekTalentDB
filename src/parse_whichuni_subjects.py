from crawldb import *
from dbtools import dict_from_row
from parsedb import ParseDB, WUSubject
from windowquery import split_process, process_db
from logger import Logger
from pprint import pprint

from sqlalchemy import func
from sqlalchemy.orm import aliased

from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

from datetime import datetime
from parse_datetime import parse_datetime
import argparse

xp_subjects = '//li[contains(@class, "result-card")]'

xp_subject_title = 'h2'
xp_subject_description = 'h3'
xp_subject_url = 'div[@class="result-card__link-container"]/a'
xp_subject_average_salary = 'div/ul[contains(@class, "result-card__outcome-salary-6months")]/li/span[@class="value"]'
xp_subject_average_salary_rating = 'div/ul[contains(@class, "result-card__outcome-salary-6months")]/li/span[contains(@class, "band")]'
xp_subject_employed_furtherstudy = 'div/ul[contains(@class, "result-card__outcome-employment-6months-facts")]/li/span[@class="value"]'
xp_subject_employed_furtherstudy_rating = 'div/ul[contains(@class, "result-card__outcome-employment-6months-facts")]/li/span[contains(@class, "band")]'
xp_subject_alevels = 'div/ul/li[contains(@class, "entry-alevel")]/span'
xp_subject_careers = 'div/ul/li[contains(@class, "popular-career")]/span'
xp_subject_courses_url = 'div/ul/li[contains(@class, "subject-link") and contains(@class, "last")]/a'


def parse_subject(element):
    d = {}
    d['title'] = extract(element, xp_subject_title, required=True)
    d['description'] = extract(element, xp_subject_description, required=True)
    d['url'] = 'http://university.which.co.uk'+ extract(element, xp_subject_url, get_attr('href'))
    d['average_salary'] = extract(element, xp_subject_average_salary)
    d['average_salary_rating'] = extract(element, xp_subject_average_salary_rating)
    d['employed_furtherstudy'] = extract(element, xp_subject_employed_furtherstudy)
    d['employed_furtherstudy_rating'] = extract(element, xp_subject_employed_furtherstudy_rating)
    d['courses_url'] = 'http://university.which.co.uk' + extract(element, xp_subject_courses_url, get_attr('href'))
    d['alevels'] = [{ "title": title } for title in extract_many(element, xp_subject_alevels)]
    d['careers'] = [{ "title": title } for title in extract_many(element, xp_subject_careers)]
    return d

def parse_page(doc):
    logger = Logger()
    d = {}
    try:
        d['subjects'] = extract_many(doc, xp_subjects, parse_subject, required=True)
    except:
        logger.log(doc.text)
        raise
    return d

def parse_pages(jobid, from_url, to_url):
    logger = Logger()
    filters = [Webpage.valid,
                Webpage.site == 'whichunisubjects',
                Webpage.url >= from_url]
    if to_url is not None:
        filters.append(Webpage.url < to_url)

    with CrawlDB() as crdb, ParseDB() as psdb:
        q = crdb.query(Webpage).filter(*filters)

        def process_row(webpage):
            try:
                doc = parse_html(webpage.html)
            except:
                return
            try:
                parsed_page = parse_page(doc)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(webpage.url))
                raise
            if parsed_page is not None:
                for subject in parsed_page['subjects']:
                    psdb.add_from_dict(subject, WUSubject)
        process_db(q, process_row, psdb, logger=logger)

def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Webpage.valid,
                Webpage.site == 'whichunisubjects']
    if args.from_url is not None:
        filters.append(Webpage.url >= args.from_url)
    
    with CrawlDB() as crdb:
        q = crdb.query(Webpage.url).filter(*filters)
        split_process(q, parse_pages, args.batch_size, 
                        njobs=args.jobs, logger=logger,
                        workdir='jobs', prefix='parse_whichuni_subjects')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-url', help=
                        'Start processing from this datoin ID. Useful for '
                        'crash recovery.')
    args = parser.parse_args()
    main(args)