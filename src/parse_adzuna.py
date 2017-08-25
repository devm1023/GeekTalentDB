from crawldb import *
from parsedb import ParseDB, ADZJob
from windowquery import split_process, process_db
from logger import Logger

from sqlalchemy import func
from sqlalchemy.orm import aliased

from htmlextract import parse_html, extract, extract_many, \
    get_attr, format_content

import re
from datetime import datetime
from parse_datetime import parse_datetime
import argparse
import json
import html


# job post xpaths
xp_microdata_json = '//script[@type="application/ld+json"]'
xp_microdata_root = '//*[@itemscope and (@itemtype = "http://schema.org/JobPosting" or @itemtype = "https://schema.org/JobPosting")]'
xp_microdata_description = './/*[@itemprop="description"]'

def check_job_post(url, redirect_url, timestamp, doc, logger=Logger(None)):

    return True


def parse_job_post(url, redirect_url, timestamp, tag, doc):
    logger = Logger()
    if not check_job_post(url, redirect_url, timestamp, doc, logger=logger):
        return None
    
    d = {'url' : redirect_url,
         'timestamp' : timestamp,
         'adref' : tag}

    # check for json microdata (cwjobs, totaljobs, jobstrackr, ziprecruiter, ...)
    microdata_json_tags = doc.xpath(xp_microdata_json)
    for el in microdata_json_tags:
        data = json.loads(html.unescape(el.text.replace('\n', '').replace('&quot;', '\\"')))
        if data['@type'].lower() != 'jobposting':
            continue

        d['description'] = format_content(parse_html(data['description']))
        

    # check for microdata (adzuna, reed and others)
    if 'description' not in d:
        microdata_root = doc.xpath(xp_microdata_root)
        if len(microdata_root) == 1:
            d['description'] = extract(microdata_root[0], xp_microdata_description, format_content)
        
    return d


def parse_job_posts(jobid, from_url, to_url, from_ts, to_ts):
    logger = Logger()
    filters = [Webpage.valid,
               Webpage.site == 'adzuna',
               Webpage.redirect_url >= from_url]
    if to_url is not None:
        filters.append(Webpage.redirect_url < to_url)
    
    with CrawlDB() as crdb, ParseDB() as psdb:
        # construct query that retreives the latest version of each profile
        maxts = func.max(Webpage.timestamp) \
                    .over(partition_by=Webpage.redirect_url) \
                    .label('maxts')
        subq = crdb.query(Webpage, maxts) \
                   .filter(*filters) \
                   .subquery()
        WebpageAlias = aliased(Webpage, subq)
        q = crdb.query(WebpageAlias) \
                .filter(subq.c.timestamp == subq.c.maxts,
                        subq.c.type.in_(['post', 'external-post']))
        if from_ts is not None:
            q = q.filter(subq.c.maxts >= from_ts)
        if to_ts is not None:
            q = q.filter(subq.c.maxts < to_ts)

        # this function does the parsing
        def process_row(webpage):
            try:
                doc = parse_html(webpage.html)
            except:
                return
            try:
                parsed_job_post = parse_job_post(
                    webpage.url, webpage.redirect_url, webpage.timestamp, webpage.tag, doc)
            except:
                logger.log('Error parsing HTML from URL {0:s}\n' \
                           .format(webpage.url))
                raise
            if parsed_job_post is not None:
                psdb.add_from_dict(parsed_job_post, ADZJob)

        # apply process_row to each row returned by q and commit to psdb
        # in regular intervals
        process_db(q, process_row, psdb, logger=logger)


def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Webpage.valid,
               Webpage.site == 'adzuna']
    from_ts = parse_datetime(args.from_timestamp)
    if from_ts:
        filters.append(Webpage.timestamp >= from_ts)
    to_ts = parse_datetime(args.to_timestamp)
    if to_ts:
        filters.append(Webpage.timestamp < to_ts)
    if args.from_url is not None:
        filters.append(Webpage.redirect_url >= args.from_url)
    
    with CrawlDB() as crdb:
        # construct a query which returns the redirect_url fields of the pages
        # to process. This query is only used to partition the data. The actual
        # processing happens in parse_job_posts.
        q = crdb.query(Webpage.redirect_url).filter(*filters)

        split_process(q, parse_job_posts, args.batch_size,
                      args=[from_ts, to_ts], njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='parse_adzuna')
            


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
    args = parser.parse_args()
    main(args)
    
