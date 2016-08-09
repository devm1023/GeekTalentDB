from crawldb import *
from parsedb import ParseDB, LIProfile
from windowquery import split_process, process_db
from logger import Logger

from sqlalchemy import func
from sqlalchemy.orm import aliased

from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

import re
from datetime import datetime
import argparse


# profile xpaths
xp_picture  = ('//*[@id="topcard"]/div[@class="profile-card vcard"]'
               '/div[@class="profile-picture"]/a/img')
xp_name     = '//*[@id="name"]'
xp_title    = '//*[@class="headline title"]'
xp_location = '//*[@id="demographics"]/dd[@class="descriptor adr"]/span'
xp_sector   = '//*[@id="demographics"]/dd[@class="descriptor"]/span'
xp_description = '//*[@id="summary"]/div[@class="description"]'
xp_connections = '//*[@class="member-connections"]/strong'
xp_skills = ['//*[@id="skills"]/ul/li[@class="skill"]',
             '//*[@id="skills"]/ul/li[@class="skill extra"]']
xp_experiences = ('//*[@id="experience"]/ul[@class="positions"]'
                  '/li[@class="position"]')
xp_educations = '//*[@id="education"]/ul/li'

# skill xpaths
xp_skill_url = 'a'
xp_skill_name = './/span'

# experience xpaths
xp_exp_title = 'header/h4[@class="item-title"]'
xp_exp_company_url = 'header/h5[@class="item-subtitle"]/a'
xp_exp_company = 'header/h5[@class="item-subtitle"]'
xp_exp_daterange = 'div[@class="meta"]/span[@class="date-range"]/time'
xp_exp_location = 'div[@class="meta"]/span[@class="location"]'
xp_exp_description = '*[@class="description"]'

# education xpaths
xp_edu_institute = 'header/*[@class="item-title"]'
xp_edu_url = 'header/*[@class="item-title"]/a'
xp_edu_course = 'header/*[@class="item-subtitle"]'
xp_edu_daterange = 'div[@class="meta"]/span[@class="date-range"]/time'
xp_edu_description = '*[@class="description"]'


# login page url regexp
re_login = re.compile(r'^https?://(www|[a-z][a-z])\.linkedin\.com(/hp)?/?$')


def check_profile(url, redirect_url, timestamp, doc, logger=Logger(None)):
    title_elem = doc.xpath('/html/head/title')
    invalid_titles = [
        '999: request failed',
        'Profile Not Found | LinkedIn']
    if not title_elem or title_elem[0].text in invalid_titles:
        return False

    if re_login.match(redirect_url):
        logger.log('Got login page for URL {0:s}\n'.format(url))
        return False

    if not doc.xpath(xp_name):
        logger.log('Missing name field for URL {0:s}\n'.format(url))
        return False
    
    return True


def parse_skill(element):
    d = {}
    url = extract(element, xp_skill_url, get_attr('href'))
    d['url'] = url.split('?')[0] if url else None
    d['name'] = extract(element, xp_skill_name, required=True)

    return d


def parse_experience(element):
    d = {}
    d['current'] = element.get('data-section') == 'currentPositionsDetails'
    d['title'] = extract(element, xp_exp_title, format_content, required=True)
    d['company'] = extract(element, xp_exp_company, format_content)
    url = extract(element, xp_exp_company_url, get_attr('href'))
    d['company_url'] = url.split('?')[0] if url else None
    daterange = extract_many(element, xp_exp_daterange)
    d['start'] = d['end'] = None
    if len(daterange) > 0:
        d['start'] = daterange[0]
    if len(daterange) > 1:
        d['end'] = daterange[1]
    if len(daterange) > 2:
        raise ValueError('Too many <time> tags in date range.')
    d['location'] = extract(element, xp_exp_location)
    d['description'] = extract(element, xp_exp_description, format_content)

    return d


def parse_education(element):
    d = {}
    d['institute'] = extract(element, xp_edu_institute, format_content)
    url = extract(element, xp_edu_url, get_attr('href'))
    d['url'] = url.split('?')[0] if url else None
    d['course'] = extract(element, xp_edu_course, format_content)
    daterange = extract_many(element, xp_edu_daterange)
    d['start'] = d['end'] = None
    if len(daterange) > 0:
        d['start'] = daterange[0]
    if len(daterange) > 1:
        d['end'] = daterange[1]
    if len(daterange) > 2:
        raise ValueError('Too many <time> tags in date range.')
    d['description'] = extract(element, xp_edu_description, format_content)

    return d


def parse_profile(url, redirect_url, timestamp, doc):
    logger = Logger()
    if not check_profile(url, redirect_url, timestamp, doc, logger=logger):
        return None
    
    d = {'url' : redirect_url,
         'timestamp' : timestamp}

    d['picture_url'] = extract(doc, xp_picture, get_attr('data-delayed-url'))
    d['name'] = extract(doc, xp_name, required=True)
    d['location'] = extract(doc, xp_location)
    d['sector'] = extract(doc, xp_sector)
    d['title'] = extract(doc, xp_title, format_content)
    d['description'] = extract(doc, xp_description, format_content)
    d['connections'] = extract(doc, xp_connections)

    d['skills'] = extract_many(doc, xp_skills, parse_skill)
    d['experiences'] = extract_many(doc, xp_experiences, parse_experience)
    d['educations'] = extract_many(doc, xp_educations, parse_education)
    
    return d


def parse_profiles(jobid, from_url, to_url, from_ts, to_ts):
    logger = Logger()
    filters = [Website.valid,
               Website.leaf,
               Website.site == 'linkedin',
               Website.url >= from_url]
    if to_url is not None:
        filters.append(Website.url < to_url)
    
    with CrawlDB() as crdb, ParseDB() as psdb:
        maxts = func.max(Website.timestamp) \
                    .over(partition_by=Website.timestamp) \
                    .label('maxts')
        subq = crdb.query(Website, maxts) \
                   .filter(*filters) \
                   .subquery()
        WebsiteAlias = aliased(Website, subq)
        q = crdb.query(WebsiteAlias) \
                .filter(subq.c.timestamp == subq.c.maxts)
        if from_ts is not None:
            q = q.filter(subq.c.maxts >= from_ts)
        if to_ts is not None:
            q = q.filter(subq.c.maxts < to_ts)

        def process_row(website):
            doc = parse_html(website.html)
            try:
                parsed_profile = parse_profile(
                    website.url, website.redirect_url, website.timestamp, doc)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(website.url))
                raise
            if parsed_profile is not None:
                psdb.add_from_dict(parsed_profile, LIProfile)
            
        process_db(q, process_row, psdb, logger=logger)


def make_datetime(date):
    if date is None:
        return None
    if '_' in date:
        return datetime.strptime(date, '%Y-%m-%d_%H:%M:%S')
    return datetime.strptime(date, '%Y-%m-%d')


def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Website.valid,
               Website.leaf,
               Website.site == 'linkedin']
    from_ts = make_datetime(args.from_date)
    if from_ts:
        filters.append(Website.timestamp >= from_ts)
    to_ts = make_datetime(args.to_date)
    if to_ts:
        filters.append(Website.timestamp < to_ts)
    if args.from_url is not None:
        filters.append(Website.url >= args.from_url)
    
    with CrawlDB() as crdb:
        q = crdb.query(Website.url).filter(*filters)

        split_process(q, parse_profiles, args.batch_size,
                      args=[from_ts, to_ts], njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='parse_linkedin')
            


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-date',
                        help='Start of date range. Format: YYYY-MM-DD_HH:MM:SS')
    parser.add_argument('--to-date',
                        help='End of date range. Format: YYYY-MM-DD_HH:MM:SS')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-url', help=
                        'Start processing from this datoin ID. Useful for '
                        'crash recovery.')
    args = parser.parse_args()
    main(args)
    
