from crawldb import *
from parse_datetime import parse_datetime
from windowquery import split_process, process_db, collapse
from logger import Logger
from sqlalchemy import func
import argparse


def update_urls(jobid, from_url, to_url,
                site, types, exclude_types, from_ts, to_ts):
    logger = Logger()
    with CrawlDB() as crdb:
        q = crdb.query(Webpage.url) \
                .filter(Webpage.site == site,
                        Webpage.valid,
                        Webpage.url >= from_url)
        if to_url is not None:
            q = q.filter(Webpage.url < to_url)
        if types:
            q = q.filter(Webpage.type.in_(types))
        if exclude_types:
            q = q.filter(~Webpage.type.in_(exclude_types))
        if from_ts:
            q = q.filter(Webpage.timestamp >= from_ts)
        if to_ts:
            q = q.filter(Webpage.timestamp < to_ts)
        subq = q.subquery()

        q = crdb.query(Webpage.url, Webpage) \
                .filter(Webpage.url.in_(subq)) \
                .order_by(Webpage.url, Webpage.timestamp)

        def do_update(row):
            url, webpages = row
            webpages = [w for w, in webpages]
            new_url = None
            if webpages and webpages[-1].valid:
                new_url = webpages[-1].redirect_url
            elif len(webpages) > 1 and webpages[-2].valid:
                new_url = webpages[-2].redirect_url

            if new_url:
                for webpage in webpages:
                    logger.log('Updating {0:s} to {1:s}\n' \
                               .format(url, new_url))
                    webpage.url = new_url
            
        process_db(collapse(q), do_update, crdb, logger=logger)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs. Default: 1')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of urls to add in one batch. '
                        'Default: 1000')
    parser.add_argument('--types', default=None,
                        help='Comma-separated list of page types to update. '
                        'If not specified all pages are harvested.')
    parser.add_argument('--exclude-types', default=None,
                        help='Comma-separated list of page types to exclude '
                        'from update.')
    parser.add_argument('--from-timestamp',
                        help='Only update pages with timstamp greater '
                        'or equal to FROM_TIMESTAMP')
    parser.add_argument('--to-timestamp',
                        help='Only update pages with timstamp less than '
                        'TO_TIMESTAMP')
    parser.add_argument('site',
                        help='ID string of the site to update.')
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
        q = crdb.query(Webpage.url) \
                .filter(Webpage.site == site,
                        Webpage.valid)
        if types:
            q = q.filter(Webpage.type.in_(types))
        if exclude_types:
            q = q.filter(~Webpage.type.in_(exclude_types))
        if from_timestamp:
            q = q.filter(Webpage.timestamp >= from_timestamp)
        if to_timestamp:
            q = q.filter(Webpage.timestamp < to_timestamp)

        split_process(q, update_urls, batchsize=args.batch_size,
                      njobs=args.jobs,
                      args=[site, types, exclude_types, from_timestamp,
                            to_timestamp],
                      logger=logger, workdir='jobs',
                      prefix='crawl_update_urls')

    
