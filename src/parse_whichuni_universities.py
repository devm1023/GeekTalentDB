import argparse
import re
from datetime import datetime
from pprint import pprint

from crawldb import *
from dbtools import dict_from_row
from htmlextract import (extract, extract_many, format_content, get_attr,
                         get_stripped_text, parse_html)
from logger import Logger
from parse_datetime import parse_datetime
from parsedb import ParseDB
from sqlalchemy import func
from sqlalchemy.orm import aliased
from windowquery import process_db, split_process



xp_uni_name = '//h1'
xp_city = '//div[@class="grid--row"]/h4'
xp_ucas_code = '//p[contains(., "Ucas code: ")]'
xp_courses_url = '//div[contains(@class, "view-all-courses")]/a'
xp_description = '//div[contains(@class, "institution-text")]'
xp_website_url = '//a[@class="ext-university-link"]'
xp_further_study = '//div[@id="significant-employment_ratio"]/../span[@class="figure"]'
xp_further_study_r = '//div[@id="significant-employment_ratio"]/../span[contains(@class, "banding")]'
xp_average_salary = '//div[@id="significant-avg_salary_sixmonths"]/../span[@class="figure"]'
xp_average_salary_r = '//div[@id="significant-avg_salary_sixmonths"]/../span[contains(@class, "banding")]'
xp_student_score = '//div[@id="significant-student_score"]/../span[@class="figure"]'
xp_student_score_r = '//div[@id="significant-student_score"]/../span[contains(@class, "banding")]'
xp_satisfaction = '//h3[contains(.,"Overall student satisfaction")]/../span'
xp_no_of_students = '//h3[contains(.,"Number of students")]/../span'

def get_int_from_percentage(element):
    return int(re.sub(r'\D', '', element.text))

def get_salary(element):
    salary = float(re.sub(r'£|k', '', element.text))
    return int(salary * 1000)

def get_num_from_string(element):
    return int(re.sub(r'\,', '', element.text))

def parse_page(doc, url):
    logger = Logger()
    d = {}
    try:
        no_of_students_elems = extract_many(doc, xp_no_of_students, get_num_from_string)
        if len(no_of_students_elems) == 0:
            no_of_students_elems = None
        else:
            no_of_students_elems = no_of_students_elems[0]
        d['name'] = extract(doc, xp_uni_name, required=True)
        d['city'] = { "name": extract(doc, xp_city).split('|').pop().strip() }
        d['url'] = url
        d['ucas_code'] = extract(doc, xp_ucas_code).split(':').pop().strip()
        d['courses_url'] = 'http://university.which.co.uk' + extract(doc, xp_courses_url, get_attr('href'))
        d['description'] = extract(doc, xp_description)
        d['website'] = extract_many(doc, xp_website_url, get_attr('href'))[0]
        d['further_study'] = extract(doc, xp_further_study, get_int_from_percentage)
        d['further_study_r'] = extract(doc, xp_further_study_r)
        d['average_salary'] = extract(doc, xp_average_salary, get_salary)
        d['average_salary_r'] = extract(doc, xp_average_salary_r)
        d['student_score'] = extract(doc, xp_student_score, get_int_from_percentage)
        d['student_score_r'] = extract(doc, xp_student_score_r)
        d['satisfaction'] = extract(doc, xp_satisfaction, get_int_from_percentage)
        d['no_of_students'] = no_of_students_elems
        
        pprint(d)
    except Exception as e:
        raise RuntimeError('URL {0} failed\n{1}\n'.format(url, str(e)))


def parse_pages(jobid, from_url, to_url):
    logger = Logger()
    filters = [Webpage.valid,
                Webpage.site == 'whichuni',
                Webpage.type == 'university-profile',
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
                parsed_page = parse_page(doc, webpage.url)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(webpage.url))
                raise
            #if parsed_page is not None:
                # psdb.add_from_dict(parsed_page, WUUniversity)
        process_db(q, process_row, psdb, logger=logger)

def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Webpage.valid,
                Webpage.site == 'whichuni',
                Webpage.type == 'university-profile']
    if args.from_url is not None:
        filters.append(Webpage.url >= args.from_url)
    
    with CrawlDB() as crdb:
        q = crdb.query(Webpage.url).filter(*filters)
        split_process(q, parse_pages, args.batch_size, 
                        njobs=args.jobs, logger=logger,
                        workdir='jobs', prefix='parse_whichuni_universities')


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
