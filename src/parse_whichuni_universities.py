import argparse
import re
from datetime import datetime
from pprint import pprint

import lxml
from crawldb import *
from dbtools import dict_from_row
from htmlextract import (extract, extract_many, format_content, get_attr,
                         get_stripped_text, parse_html)
from logger import Logger
from parse_datetime import parse_datetime
from parsedb import ParseDB, WUUniversity
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

xp_grad_undergrad = '//h3[contains(.,"Undergraduate / Postgraduate")]/..'
xp_grad_undergrad_text = 'p'
xp_grad_undergrad_value = 'span'

xp_work_time = '//h3[contains(.,"Full-time / Part-time")]/..'
xp_work_time_value = 'span'
xp_work_time_text = 'p'

xp_male_female = '//h3[contains(.,"Male / Female")]/..'
xp_male_female_value = 'span'
xp_male_female_text = 'p'

xp_young_mature = '//h3[contains(.,"Young / Mature")]/..'
xp_young_mature_value = 'span'
xp_young_mature_text = 'p'

xp_uk_nonuk = '//h3[contains(.,"UK / Non-UK")]/..'
xp_uk_nonuk_value = 'span'
xp_uk_nonuk_text = 'p'

xp_lg_tables = '//div[@class="ranks"]'
xp_lg_table_value = 'div/div/div/div'
xp_lg_table_ttl = 'div/div/div/span/span'

xp_tags = '//div[@class="character-tags"]/span'

xp_characteristics = '//div[@class="rating-cards"]'
xp_characteristic_name = 'div/div/h3'
xp_characteristic_value = 'div/div/h4'
xp_characteristic_r = 'div/div/span'

def get_characteristic(element):
    name = extract(element, xp_characteristic_name)
    value = extract(element, xp_characteristic_value)
    if value is None:
        return value
    if len(value) == 3 or len(value) == 4:
        value = extract(element, xp_characteristic_value, get_int_from_percentage)
    else:
        return None
    rating = extract(element, xp_characteristic_r)
    return {
        'name': name,
        'score': value,
        'rating': rating
    }

def get_ranks(element):
    value = extract(element, xp_lg_table_value)
    total = extract(element, xp_lg_table_ttl)
    return [value, total]

def get_int_from_percentage(element):
    return int(re.sub(r'\D', '', element.text))

def get_salary(element):
    salary = float(re.sub(r'£|k', '', element.text))
    return int(salary * 1000)

def get_num_from_string(element):
    return int(re.sub(r'\,', '', element.text))

def get_grad_percent(element):
    text = extract(element, xp_grad_undergrad_text)
    value = extract(element, xp_grad_undergrad_value, get_int_from_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students are undergrads":
            return [value, 100-value]
        else:
            return [100-value, value]

def get_work_time(element):
    text = extract(element, xp_work_time_text)
    value = extract(element, xp_work_time_value, get_int_from_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students are part-time":
            return [value, 100-value]
        else:
            return [100-value, value]

def get_male_female(element):
    text = extract(element, xp_male_female_text)
    value = extract(element, xp_male_female_value, get_int_from_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students are female":
            return [100-value, value]
        else:
            return [value, 100-value]

def get_young_mature(element):
    text = extract(element, xp_young_mature_text)
    value = extract(element, xp_young_mature_value, get_int_from_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students aged over 21":
            return [100-value, value]
        else:
            return [value, 100-value]

def get_uk_nonuk(element):
    text = extract(element, xp_uk_nonuk_text)
    value = extract(element, xp_uk_nonuk_value, get_int_from_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students here are from outside the UK":
            return [100-value, value]
        else:
            return [value, 100-value]

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
        d['city'] = { "name": extract(doc, xp_city).split('|')[0].strip() }
        d['url'] = url
        d['ucas_code'] = extract(doc, xp_ucas_code).split(':')[1].strip()
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
        grad_undergrad_percent = extract(doc, xp_grad_undergrad, get_grad_percent)
        if grad_undergrad_percent is not None:
            d['postgraduate'] = grad_undergrad_percent[1]
            d['undergraduate'] = grad_undergrad_percent[0]
        part_time_full_time = extract(doc, xp_work_time, get_work_time)
        if part_time_full_time is not None:
            d['part_time'] = part_time_full_time[0]
            d['full_time'] = part_time_full_time[1]
        male_female = extract(doc, xp_male_female, get_male_female)
        if male_female is not None:
            d['male'] = male_female[0]
            d['female'] = male_female[1]
        young_mature = extract(doc, xp_young_mature, get_young_mature)
        if young_mature is not None:
            d['young'] = young_mature[0]
            d['mature'] = young_mature[1]
        uk_nonuk = extract(doc, xp_uk_nonuk, get_uk_nonuk)
        if uk_nonuk is not None:
            d['uk'] = uk_nonuk[0]
            d['non_uk'] = uk_nonuk[1]
        ranks = extract_many(doc, xp_lg_tables, get_ranks)
        if ranks is not None and len(ranks) > 0:
            d['lg_table_0'] = ranks[0][0]
            d['lg_table_0_ttl'] = ranks[0][1]
            if len(ranks) >= 2:
                d['lg_table_1'] = ranks[1][0]
                d['lg_table_1_ttl'] = ranks[1][1]
                if len(ranks) >= 3:
                    d['lg_table_2'] = ranks[2][0]
                    d['lg_table_2_ttl'] = ranks[2][1]
        d['tags'] = [{ "name": tag } for tag in extract_many(doc, xp_tags)]
        d['characteristics'] = extract_many(doc, xp_characteristics, get_characteristic)
        return d
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
            if parsed_page is not None:
                pprint(parsed_page)
                psdb.add_from_dict(parsed_page, WUUniversity)
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
