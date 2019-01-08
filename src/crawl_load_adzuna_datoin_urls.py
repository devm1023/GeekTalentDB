import argparse
import csv
from datetime import datetime

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
    args = parser.parse_args()

    # parse dates
    try:
        args.from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        if not args.to_date:
            args.to_date = datetime.now()
        else:
            args.to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError:
        sys.stderr.write('Error: Invalid date.\n')
        exit(1)

    logger = Logger()
    batch_size = 10000
    site = 'adzuna'
    type = None

    with CrawlDB() as crdb, DatoinDB() as dtdb:
        q = dtdb.query(ADZJob.redirect_url, ADZJob.adz_id) \
            .filter(ADZJob.created >= args.from_date,
                    ADZJob.created < args.to_date)

        count = 0
        for redirect_url, adz_id in q:

            # jobkey already exists, skip
            if crdb.query(Webpage.id).filter(Webpage.site == site, Webpage.tag == adz_id).first() is not None:
                continue

            count += 1

            crdb.add_url(site, type, redirect_url, adz_id)
            if count % batch_size == 0:
                crdb.commit()
                logger.log('{0:d} URLs loaded.\n'.format(count))

        if count % batch_size != 0:
            logger.log('{0:d} URLs loaded.\n'.format(count))
        crdb.commit()



