import argparse
import csv
from datetime import datetime
import time
import math

from crawldb import *
from datoindb import DatoinDB, ADZJob
from logger import Logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--from-date', help=
                        'Only load urls from posts created after this date'
                         '. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only load urls from posts created before this date'
                        '. Format: YYYY-MM-DD')
    parser.add_argument('--category', help=
                        'categories to load specific URLs', default=None)
    parser.add_argument('--country', help=
                        'countries to load specific URLs', default=None)
    args = parser.parse_args()

    # parse dates
    try:
        args.from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        args.from_date = time.mktime(args.from_date.timetuple())
        if not args.to_date:
            args.to_date = time.mktime(datetime.now().timetuple())
        else:
            args.to_date = time.mktime(datetime.strptime(args.to_date, '%Y-%m-%d').timetuple())
    except ValueError:
        sys.stderr.write('Error: Invalid date.\n')
        exit(1)

    logger = Logger()
    batch_size = 10000
    site = 'adzuna'
    type = None

    with CrawlDB() as crdb, DatoinDB() as dtdb:
        q = dtdb.query(ADZJob.redirect_url, ADZJob.adz_id, ADZJob.category, ADZJob.country) \
            .filter(ADZJob.crawled_date >= math.floor(args.from_date),
                    ADZJob.crawled_date < math.floor(args.to_date))

        if args.category is not None:
            q = q.filter(ADZJob.category == args.category)
        if args.country is not None:
            q = q.filter(ADZJob.country == args.country)

        count = 0
        for redirect_url, adz_id, category, country in q:

            # jobkey already exists, skip
            subq = crdb.query(Webpage.id).filter(Webpage.site == site, Webpage.tag == adz_id).first()
            if subq is not None:
               continue

            count += 1

            crdb.add_url(site, type, redirect_url, adz_id, category, country)
            if count % batch_size == 0:
                crdb.commit()
                logger.log('{0:d} URLs loaded.\n'.format(count))

        if count % batch_size != 0:
            logger.log('{0:d} URLs loaded.\n'.format(count))
        crdb.commit()



