from crawldb import *
from parsedb import ParseDB, INJob
from datoindb import DatoinDB, IndeedJob
from windowquery import split_process, process_db
from phraseextract import PhraseExtractor
from textnormalization import tokenized_skill
from logger import Logger

from sqlalchemy import func
from sqlalchemy.orm import aliased

from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

import re
from datetime import datetime
from parse_datetime import parse_datetime
import argparse
import csv
import json
import html
import langdetect

# job post xpaths
xp_job_description = '//*[@id="job_summary"]'
xp_job_description2 = '//*[contains(@class, "jobsearch-JobComponent ")]'

def check_job_post(url, redirect_url, timestamp, doc, logger=Logger(None)):

    return True


def parse_job_post(url, redirect_url, timestamp, tag, doc, skillextractors):
    logger = Logger()
    if not check_job_post(url, redirect_url, timestamp, doc, logger=logger):
        return None

    d = {'url' : redirect_url,
         'timestamp' : timestamp,
         'jobkey' : tag,
         'description': None}

    d['description'] = extract(doc, xp_job_description, format_content)

    if d['description'] is None:
        d['description'] = extract(doc, xp_job_description2, format_content)

    if d['description'] is not None:
        language = langdetect.detect(d['description'])

        # extract skills
        if skillextractors is not None and language in skillextractors:
            text = ' '.join(s for s in [d['description']] \
                        if s)
            extracted_skills = list(set(skillextractors[language](text)))

            d['skills'] = [{'name': s} for s in extracted_skills]

    return d


def parse_job_posts(jobid, from_url, to_url, from_ts, to_ts, category, skillextractors, country):
    logger = Logger()
    filters = [Webpage.valid,
               Webpage.site == 'indeedjob',
               Webpage.redirect_url >= from_url,
               Webpage.country == country,
               Webpage.category == category]
    if to_url is not None:
        filters.append(Webpage.redirect_url < to_url)
    
    with CrawlDB() as crdb, ParseDB() as psdb, DatoinDB() as dtdb:
        # construct query that retreives the latest version of each profile
        maxts = func.max(Webpage.timestamp) \
                    .over(partition_by=Webpage.redirect_url) \
                    .label('maxts')
        subq = crdb.query(Webpage, maxts) \
                   .filter(*filters) \
                   .subquery()
        WebpageAlias = aliased(Webpage, subq)
        q = crdb.query(WebpageAlias) \
                .filter(subq.c.timestamp == subq.c.maxts)
        if from_ts is not None:
            q = q.filter(subq.c.maxts >= from_ts)
        if to_ts is not None:
            q = q.filter(subq.c.maxts < to_ts)

        # this function does the parsing
        def process_row(webpage):

            # category must be passed in now - removed section for testing
            #if category is not None:
            #    post_category = dtdb.query(IndeedJob.category).filter(IndeedJob.jobkey == webpage.tag).first()
            #    if post_category is None or post_category[0] != category:
            #        return

            try:
                doc = parse_html(webpage.html)
            except:
                return
            try:
                parsed_job_post = parse_job_post(
                    webpage.url, webpage.redirect_url, webpage.timestamp, webpage.tag, doc, skillextractors)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(webpage.url))
                raise
            if parsed_job_post is not None:
                psdb.add_from_dict(parsed_job_post, INJob)

        # apply process_row to each row returned by q and commit to psdb
        # in regular intervals
        process_db(q, process_row, psdb, logger=logger)


def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Webpage.valid,
               Webpage.site == 'indeedjob']
    from_ts = parse_datetime(args.from_timestamp)
    skillfile = args.skills

    if from_ts:
        filters.append(Webpage.timestamp >= from_ts)
    to_ts = parse_datetime(args.to_timestamp)
    if to_ts:
        filters.append(Webpage.timestamp < to_ts)
    if args.from_url is not None:
        filters.append(Webpage.redirect_url >= args.from_url)

    skillextractors = None
    if skillfile is not None:
        skills = {}
        with open(skillfile, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if row:
                    if len(row) == 1:
                        lang = 'en'
                        skill = row[0]
                    else:
                        lang = row[0]
                        skill = row[1]

                    if lang not in skills:
                        skills[lang] = []

                    skills[lang].append(skill)

        skillextractors = {}
        for lang in skills.keys():
            tokenize = lambda x: tokenized_skill(lang, x)
            skillextractors[lang] = PhraseExtractor(skills[lang], tokenize=tokenize, margin=2.0, fraction=1.0)

        del skills

    with CrawlDB() as crdb:
        # construct a query which returns the redirect_url fields of the pages
        # to process. This query is only used to partition the data. The actual
        # processing happens in parse_job_posts.
        q = crdb.query(Webpage.redirect_url).filter(*filters)

        split_process(q, parse_job_posts, args.batch_size,
                      args=[from_ts, to_ts, args.category, skillextractors], njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='parse_indeed_job')
            


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-timestamp',
                        help='Start of timestamp range.')
    parser.add_argument('--to-timestamp',
                        help='End of timestamp range.')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-url', help=
                        'Start processing from this datoin ID. Useful for '
                        'crash recovery.')
    parser.add_argument('--skills', help=
                        'Name of a CSV file holding skill tags.')
    parser.add_argument('--category', type=str, default=None,
                        help='Category for jobs. e.g. it-jobs')
    parser.add_argument('--country', type=str, default=None,
                        help='Country for jobs. e.g. gb')
    args = parser.parse_args()
    main(args)
    
