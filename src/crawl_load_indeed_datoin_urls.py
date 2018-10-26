import argparse
import csv
from datetime import datetime

from crawldb import *
from datoindb import DatoinDB, IndeedJob
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
    site = 'indeedjob'
    type = None

    with CrawlDB() as crdb, DatoinDB() as dtdb:
        q = dtdb.query(IndeedJob.url, IndeedJob.jobkey) \
                .filter(IndeedJob.date >= args.from_date,
                        IndeedJob.date < args.to_date)

        count = 0
        for url, jobkey in q:

            # jobkey already exists, skip
            if crdb.query(Webpage.id).filter(Webpage.site == site, Webpage.tag == jobkey).first() is not None:
                continue

            count += 1

            crdb.add_url(site, type, url, jobkey)
            if count % batch_size == 0:
                crdb.commit()
                logger.log('{0:d} URLs loaded.\n'.format(count))
        
        if count % batch_size != 0:
            logger.log('{0:d} URLs loaded.\n'.format(count))
        crdb.commit()



