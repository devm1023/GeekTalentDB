import argparse
import re
from datetime import datetime
from pprint import pprint

from crawldb import *
from dbtools import dict_from_row
from htmlextract import (extract, extract_many, format_content, get_attr,
                         parse_html, get_stripped_text)
from logger import Logger
from parse_datetime import parse_datetime
from parsedb import ParseDB, WUCourse, WUUniversitySubject
from sqlalchemy import func
from sqlalchemy.orm import aliased
from windowquery import process_db, split_process

xp_name = '//h1'
xp_ucas_code = '//span[@class="snippet-mode"]'
xp_university_name = '//h3[@class="institution name"]/a'
xp_ucas_points = '//span[contains(., "Ucas points guide")]/../div/p'
xp_offers = '//span[contains(., "% applicants receiving offers")]/../div/p'
xp_tuition_fee = '//div[contains(@class, "financial")]/span'
xp_description = '//p[@id="course-summary"]'
xp_modules = '//p[@id="course-modules"]'

xp_study_types = '//div[@class="course-snippets"]'
xp_qualification = 'span[@class="snippet qualification"]'
xp_duration_years = 'span[@class="snippet duration"]'
xp_study_mode = 'span[@class="snippet studymode"]'
xp_academic_year = 'span[@class="snippet academic_year"]'

xp_entry_requirements = '//div[@class="entry_requirement_outer_container"]'
xp_entry_requirement_name = 'div/div/span[@class="scheme"]'
xp_entry_requirement_value = 'div/div/span[@class="value"]'
xp_entry_requirement_text = 'div/p'

xp_subjects = '//div[contains(@class, "tab-pane")]'

xp_subject_name = 'div[contains(@class, "title")]'
xp_subject_student_score = 'div/div/div/div/div/div/div[@class="content"]/div/div[@id="student-score"]/../span[@class="figure"]'
xp_subject_student_score_rating = 'div/div/div/div/div/div/div[@class="content"]/div/div[@id="student-score"]/../span[contains(@class, "banding")]'
xp_subject_employed_furtherstudy = 'div/div/div/div/div/div/div[@id="employed-or-in-further-study"]/../span[@class="figure"]'
xp_subject_employed_furtherstudy_rating = 'div/div/div/div/div/div/div[@id="employed-or-in-further-study"]/../span[contains(@class, "banding")]'
xp_subject_average_salary = 'div/div/div/div/div/div/div[@id="average-graduate-salary"]/../span[@class="figure"]'
xp_subject_average_salary_rating = 'div/div/div/div/div/div/div[@id="average-graduate-salary"]/../span[contains(@class, "banding")]'
xp_subject_satisfaction = 'div/div/div/div/div/div/div[@class="content"]/div/div[@id="overall-student-satisfaction"]/../span[@class="figure"]'

xp_subject_ratings = 'div/div/div[@class="ratings"]/div[@class="rating"]'
xp_subject_rating_name = 'h5'
xp_subject_rating_rating = 'h3'

xp_uk_nonuk = 'div/div/div/div/h5[contains(.,"UK / Non-UK")]/..'
xp_uk_nonuk_value = 'div/span[@class="figure"]'
xp_uk_nonuk_text = 'div/span[@class="term"]'

xp_male_female = 'div/div/div/div/h5[contains(.,"Male / Female")]/..'
xp_male_female_text = 'div/span[@class="term"]'
xp_male_female_value = 'div/span[@class="figure"]'

xp_work_time = 'div/div/div/div/h5[contains(.,"Full-time / Part-time")]/..'
xp_work_time_value = 'div/span[@class="figure"]'
xp_work_time_text = 'div/span[@class="term"]'

xp_typical_ucas_points = 'div/div/div/div/h5[@class="fact-heading" and contains(., "Typical Ucas points")]/../div/span[@class="figure"]'
xp_twotoone_or_above = 'div/div/div/div/h5[@class="fact-heading" and contains(., "2:1 or above")]/../div/span[@class="figure"]'
xp_dropout_rate = 'div/div/div/div/h5[@class="fact-heading" and contains(., "Drop-out rate")]/../div/span[@class="figure"]'

xp_studied_before = 'div/div/div/div[@class="subject-grade"]'
xp_studied_before_text = 'div/h3[@class="title"]'
xp_studied_before_value = 'div/h3[@class="percentage"]'
xp_studied_before_grade = 'div[@class="common-grade"]/div/strong'
xp_studied_before_grade_percent = 'div[@class="common-grade"]/div/span'

xp_sectors_after = 'div/div/div/div[@class="job rating"]'
xp_sectors_after_text = 'h5'
xp_sectors_after_value = 'h3'

xp_subject_names = '//ul[@id="subject-selector"]/li/span/a'

def formatted_sectors_after(element):
    text = extract(element, xp_sectors_after_text)
    value = extract(element, xp_sectors_after_value, formatted_percentage)
    if text is None or value is None: return None
    text = text.replace('Graduates who are ', '')
    return dict({
        "name": text,
        "percent": value
    })

def formatted_studied_before(element):
    text = extract(element, xp_studied_before_text)
    percent = extract(element, xp_studied_before_value, formatted_percentage)
    common_grade = extract(element, xp_studied_before_grade)
    common_grade_percent = extract(element, xp_studied_before_grade_percent, formatted_percentage)
    if text is None or percent is None:
        return None
    else:
        return dict({
            "name": text,
            "percent": percent,
            "common_grade": common_grade,
            "common_grade_percent": common_grade_percent
        })

def formatted_work_time(element):
    text = extract(element, xp_work_time_text)
    value = extract(element, xp_work_time_value, formatted_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students are part-time":
            return [value, 100-value]
        else:
            return [100-value, value]

def formatted_male_female(element):
    text = extract(element, xp_male_female_text)
    value = extract(element, xp_male_female_value, formatted_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students are female":
            return [100-value, value]
        else:
            return [value, 100-value]

def formatted_uk_nonuk(element):
    text = extract(element, xp_uk_nonuk_text)
    value = extract(element, xp_uk_nonuk_value, formatted_percentage)
    if text is None or value is None:
        return [None, None]
    else:
        if text == "of students here are from outside the UK":
            return [100-value, value]
        else:
            return [value, 100-value]

def formatted_salary(element):
    return None if not element.text \
    or element.text == "Not Available" \
    else int(float(re.sub(r'£|k|,', '', element.text)) * 1000)

def formatted_ucas_code(element):
    return element.text.split(':')[1].strip()

def formatted_duration_years(element):
    try:
        return int(element.text.strip()[0])
    except:
        return None

def formatted_entry_rq(element):
    name = extract(element, xp_entry_requirement_name)
    if name == 'UCAS tariff points':
        return None
    else:
        grades = extract(element, xp_entry_requirement_value, check_available)
        if not grades:
            return None
        else:
            text = extract(element, xp_entry_requirement_text)
        return dict({
            "name":     name,
            "grades":   grades,
            "text":     text
        })

def formatted_ratings(element):
    return dict({
        "name": extract(element, xp_subject_rating_name),
        "rating": extract(element, xp_subject_rating_rating, formatted_percentage)
    })

def formatted_ucas_points(element):
    if not element.text:
        return None
    stripped_text = element.text.strip()
    if stripped_text == "Not Available":
        return None
    else:
        if len(stripped_text) > 3 and stripped_text.find('-') != -1:
            elements = stripped_text.split('-')
            return int(elements[0]), int(elements[1])
        else:
            first_element = int(stripped_text)
            return int(first_element),

def formatted_fee(element):
    if not element.text: return None
    fee = re.sub(r'£|k|,', '', element.text)
    return int(fee) if fee != "Not Available" else None

def value_or_none(arr, index):
    return None if not arr else (arr[index] if len(arr) >= index + 1 else None)

def formatted_percentage(element):
    if not element.text: return None
    if element.text == "Not Available": return None
    rx_text = re.sub(r'\D', '', element.text)
    if not rx_text: return None
    return int(rx_text)

def check_available(element):
    text = get_stripped_text(element)
    return text if text != "Not Available" else None

def formatted_study_types(element):
    qualification_name  = extract(element, xp_qualification)
    duration            = extract(element, xp_duration_years, formatted_duration_years)
    mode                = extract(element, xp_study_mode)
    years               = extract(element, xp_academic_year)
    return dict({
        "qualification_name":   qualification_name,
        "duration":             duration,
        "mode":                 mode,
        "years":                years
    })

def parse_course(doc, url):
    course = {}
    ucas_points = extract(doc, xp_ucas_points, formatted_ucas_points)
    subjects = extract_many(doc, xp_subject_names)
    course['url'] = url
    course['title'] = extract(doc, xp_name)
    course['ucas_code'] = extract(doc, xp_ucas_code, formatted_ucas_code)
    course['university_name'] = extract(doc, xp_university_name)
    course['study_types'] = extract_many(doc, xp_study_types, formatted_study_types)
    course['ucas_points_l'] = value_or_none(ucas_points, 0)
    course['ucas_points_h'] = value_or_none(ucas_points, 1)
    course['offers'] = extract(doc, xp_offers, formatted_percentage)
    course['entry_requirements'] = list(filter(None, extract_many(doc, xp_entry_requirements, formatted_entry_rq)))
    course['tuition_fee'] = extract(doc, xp_tuition_fee, formatted_fee)
    course['description'] = extract(doc, xp_description)
    course['modules'] = extract(doc, xp_modules)
    course['subjects'] = extract_many(doc, xp_subjects, parse_subject)
    return course

def parse_subject(doc):
    uk_nonuk = extract(doc, xp_uk_nonuk, formatted_uk_nonuk)
    male_female = extract(doc, xp_male_female, formatted_male_female)
    work_time = extract(doc, xp_work_time, formatted_work_time)
    subject = {}
    subject['subject_name'] = extract(doc, xp_subject_name)
    subject['university_name'] = extract(doc, xp_university_name)
    subject['student_score'] = extract(doc, xp_subject_student_score, formatted_percentage)
    subject['student_score_rating'] = extract(doc, xp_subject_student_score_rating)
    subject['employed_further_study'] = extract(doc, xp_subject_employed_furtherstudy, formatted_percentage)
    subject['employed_further_study_rating'] = extract(doc, xp_subject_employed_furtherstudy_rating)
    subject['average_salary'] = extract(doc, xp_subject_average_salary, formatted_salary)
    subject['average_salary_rating'] = extract(doc, xp_subject_average_salary_rating)
    subject['ratings'] = extract_many(doc, xp_subject_ratings, formatted_ratings)
    subject['uk'] = value_or_none(uk_nonuk, 0)
    subject['non_uk'] = value_or_none(uk_nonuk, 1)
    subject['male'] = value_or_none(male_female, 0)
    subject['female'] = value_or_none(male_female, 1)
    subject['part_time'] = value_or_none(work_time, 0)
    subject['full_time'] = value_or_none(work_time, 1)
    subject['typical_ucas_points'] = extract(doc, xp_typical_ucas_points, formatted_percentage)
    subject['twotoone_or_above'] = extract(doc, xp_twotoone_or_above, formatted_percentage)
    subject['satisfaction'] = extract(doc, xp_subject_satisfaction, formatted_percentage)
    subject['dropout_rate'] = extract(doc, xp_dropout_rate, formatted_percentage)
    subject['studied_before'] = extract_many(doc, xp_studied_before, formatted_studied_before)
    subject['sectors_after'] = extract_many(doc, xp_sectors_after, formatted_sectors_after)
    return subject

def parse_page(doc, url):
    logger = Logger()
    try:
        course = parse_course(doc, url)
        return course
    except Exception as e:
        raise RuntimeError('URL {0} failed\n{1}\n'.format(url, str(e)))

def parse_pages(jobid, from_url, to_url):
    logger = Logger()
    filters = [Webpage.valid,
                Webpage.site == 'whichuni',
                Webpage.type == 'course',
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
                course = parse_page(doc, webpage.url)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(webpage.url))
                raise
            if course is not None:
                psdb.add_from_dict(course, WUCourse)
        process_db(q, process_row, psdb, logger=logger)

def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Webpage.valid,
                Webpage.site == 'whichuni',
                Webpage.type == 'course']
    if args.from_url is not None:
        filters.append(Webpage.url >= args.from_url)
    
    with CrawlDB() as crdb:
        q = crdb.query(Webpage.url).filter(*filters)
        split_process(q, parse_pages, args.batch_size, 
                        njobs=args.jobs, logger=logger,
                        workdir='jobs', prefix='parse_whichuni_courses')


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
