from crawldb import *
from parse_datetime import parse_datetime
from windowquery import split_process, process_db
from logger import Logger
from sqlalchemy import func
import argparse


def add_urls(jobid, from_url, to_url,
             site, types, exclude_types, from_ts, to_ts):
    logger = Logger()
    with CrawlDB() as crdb:
        maxts = func.max(Webpage.timestamp) \
                    .over(partition_by=Link.url) \
                    .label('maxts')        
        q = crdb.query(Link.url, Link.type, Link.tag, Webpage.timestamp, maxts) \
                .join(Webpage) \
                .filter(Webpage.site == site,
                        Link.url >= from_url)
        if to_url is not None:
            q = q.filter(Link.url < to_url)
        if types:
            q = q.filter(Webpage.type.in_(types))
        if exclude_types:
            q = q.filter(~Webpage.type.in_(exclude_types))
        if from_ts:
            q = q.filter(Webpage.timestamp >= from_ts)
        if to_ts:
            q = q.filter(Webpage.timestamp < to_ts)

        subq = q.subquery()
        q = crdb.query(subq.c.url, subq.c.type, subq.c.tag) \
                .filter(((subq.c.timestamp == None) \
                         & (subq.c.maxts == None)) \
                        | (subq.c.timestamp == subq.c.maxts))

        def add_url(row):
            url, type, tag = row
            crdb.add_url(site, type, url, tag)
            
        process_db(q, add_url, crdb, logger=logger)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs. Default: 1')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of urls to add in one batch. '
                        'Default: 1000')
    parser.add_argument('--types', default=None,
                        help='Comma-separated list of page types to harvest. '
                        'If not specified all pages are harvested.')
    parser.add_argument('--exclude-types', default=None,
                        help='Comma-separated list of page types to exclude '
                        'from harvest.')
    parser.add_argument('--from-timestamp',
                        help='Only harvest pages with timstamp greater '
                        'or equal to FROM_TIMESTAMP')
    parser.add_argument('--to-timestamp',
                        help='Only harvest pages with timstamp less than '
                        'TO_TIMESTAMP')
    parser.add_argument('site',
                        help='ID string of the site to harvest.')
    args = parser.parse_args()

    logger = Logger()

    site = args.site
    types = None
    if args.types:
        types = args.types.split(',')
    exclude_types = None
    if args.exclude_types:
        exclude_types = args.exclude_types.split(',')
    from_timestamp = None
    if args.from_timestamp is not None:
        from_timestamp = parse_datetime(args.from_timestamp)
    to_timestamp = None
    if args.to_timestamp is not None:
        to_timestamp = parse_datetime(args.to_timestamp)
        
    with CrawlDB() as crdb:
        q = crdb.query(Link.url) \
                .join(Webpage) \
                .filter(Webpage.site == site)
        if types:
            q = q.filter(Webpage.type.in_(types))
        if exclude_types:
            q = q.filter(~Webpage.type.in_(exclude_types))
        if from_timestamp:
            q = q.filter(Webpage.timestamp >= from_timestamp)
        if to_timestamp:
            q = q.filter(Webpage.timestamp < to_timestamp)

        split_process(q, add_urls, batchsize=args.batch_size, njobs=args.jobs,
                      args=[site, types, exclude_types, from_timestamp,
                            to_timestamp],
                      logger=logger, workdir='jobs',
                      prefix='crawl_harvest_links')

    
