import argparse
from datetime import datetime
import sys

import conf
from canonicaldb import *
from logger import Logger
from salary_extract import extract_salary
from textnormalization import clean
from windowquery import split_process, process_db

def process_jobs(jobid, from_id, to_id,
                 from_date, to_date, source):
    cndb = CanonicalDB()
    logger = Logger(sys.stdout)

    if source == 'adzuna':
        table = ADZJob
    elif source == 'indeedjob':
        table = INJob
    else:
        raise ValueError('Invalid source type.')

    q = cndb.query(table) \
            .filter(table.crawled_on >= from_date,
                    table.crawled_on < to_date)

    q = q.filter(table.id >= from_id)

    if to_id is not None:
        q = q.filter(table.id < to_id)

    def extract_job_salary(rec):
        print(rec.id)

        try:
            salary = extract_salary(rec.full_description)

            if salary is not None:
                min_salary, max_salary, salary_period = salary
                print('\tsalary: £{} - £{} / {}'.format(min_salary, max_salary, salary_period))

                rec.salary_min, rec.salary_max, rec.salary_period = salary
            else:
                rec.salary_max = None
                rec.salary_min = None
                rec.salary_period = None
        except Exception as ex:
            print('Failed to extract salary', ex)
        
        print()

    process_db(q, extract_job_salary, cndb, logger=logger)

def run(args):
    logger = Logger(sys.stdout)
    if args.source is None:
        logger.log('Processing Adzuna salaries.\n')
        args.source = 'adzuna'
        run(args)
        logger.log('Processing Indeed salries.\n')
        args.source = 'indeedjob'
        run(args)
        return
    elif args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = INJob
    else:
        raise ValueError('Invalid source.')

    cndb = CanonicalDB()

    q = cndb.query(table.id) \
            .filter(table.crawled_on >= args.from_date,
                    table.crawled_on < args.to_date)

    split_process(q, process_jobs, args.batch_size, njobs=args.jobs,
                  args=[args.from_date, args.to_date, args.source],
                  logger=logger, workdir='jobs', prefix='canonical_extract_salaries')


def main(args):
    try:
        args.from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
        if not args.to_date:
            args.to_date = datetime.now()
        else:
            args.to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError:
        sys.stderr.write('Error: Invalid date.\n')
        exit(1)

    run(args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-date', help=
                        'Only process profiles crawled or indexed on or after '
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only process profiles crawled or indexed before\n'
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--source', choices=['adzuna', 'indeedjob'], help=
                        'Source type to process. If not specified all sources '
                        'are processed.')
    args = parser.parse_args()
    main(args)


