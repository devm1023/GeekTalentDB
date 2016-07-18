import conf
from crawldb import *
from parsedb import ParseDB, LIProfile
from windowquery import split_process, process_db
from logger import Logger

from sqlalchemy import func
from sqlalchemy.orm import aliased

from lxml import etree
from io import StringIO
import argparse

def parse_profile(url, redirect_url, timestamp, doc):
    return {'url' : redirect_url,
            'timestamp' : timestamp}


def parse_profiles(jobid, from_url, to_url):
    logger = Logger()
    html_parser = etree.HTMLParser()
    filters = [Website.valid,
               Website.leaf,
               Website.site == 'linkedin',
               Website.url >= from_url]
    if to_url is not None:
        filters.append(Website.url < to_url)
    
    with CrawlDB(conf.CRAWL_DB) as crdb, \
         ParseDB(conf.PARSE_DB) as psdb:
        maxts = func.max(Website.timestamp) \
                    .over(partition_by=Website.timestamp) \
                    .label('maxts')
        subq = crdb.query(Website, maxts) \
                   .filter(*filters) \
                   .subquery()
        WebsiteAlias = aliased(Website, subq)
        q = crdb.query(WebsiteAlias) \
                .filter(subq.c.timestamp == subq.c.maxts)

        def process_row(website):
            doc = etree.parse(StringIO(website.html), html_parser)
            parsed_profile = parse_profile(
                website.url, website.redirect_url, website.timestamp, doc)
            if parsed_profile is not None:
                psdb.add_from_dict(parsed_profile, LIProfile)
            
        process_db(q, process_row, psdb, logger=logger)
        

def main(args):
    logger = Logger()
    batch_size = args.batch_size
    filters = [Website.valid,
               Website.leaf,
               Website.site == 'linkedin']
    if args.from_url is not None:
        filters.append(Website.url >= args.from_url)
    
    with CrawlDB(conf.CRAWL_DB) as crdb:
        q = crdb.query(Website.url).filter(*filters)

        split_process(q, parse_profiles, args.batch_size,
                      njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='parse_linkedin')
            


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
    
