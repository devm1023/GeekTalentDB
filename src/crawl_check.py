"""Crawl public LinkedIn profiles.

"""

from linkedin_crawler import LinkedInCrawler
from logger import Logger
from parse_datetime import parse_datetime
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs. Default: 1')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of urls to check in one batch. '
                        'Default: 1000')
    parser.add_argument('--repair', action='store_true',
                        help='Try to repair corrupt data.')
    parser.add_argument('--from-timestamp',
                        help='Only check/repair pages with timstamp greater '
                        'or equal to FROM_TIMESTAMP')
    parser.add_argument('--to-timestamp',
                        help='Only check/repair pages with timstamp less than '
                        'TO_TIMESTAMP')
    parser.add_argument('site',
                        help='String ID of the site to check')
    args = parser.parse_args()
    logger = Logger()

    if args.site == 'linkedin':
        Crawler = LinkedInCrawler
    else:
        raise SystemExit('Unknown site `{0:s}`'.format(args.site))

    from_timestamp = None
    if args.from_timestamp is not None:
        from_timestamp = parse_datetime(args.from_timestamp)
    to_timestamp = None
    if args.to_timestamp is not None:
        to_timestamp = parse_datetime(args.to_timestamp)
    
    crawler = Crawler(
        jobs=args.jobs,
        batch_size=args.batch_size,
        workdir='crawljobs',
        prefix='crawl_check',
        logger=logger)

    crawler.check_db(repair=args.repair,
                     from_timestamp=from_timestamp,
                     to_timestamp=to_timestamp)
    
