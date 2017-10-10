import argparse

from datoindb import *
from windowquery import split_process, process_db
from logger import Logger
from htmlextract import format_content, parse_html
from parsedb import ParseDB, INJob as ParseINJob



"""
    Script prepares a copy of full description field for each job which will be used for identifying duplicates.
    Following formatting is applied to full description text: html strip, whitespace normalisation, lowercasing.
"""

def sanitize(txt):
    txt = format_content(parse_html(txt))
    return ' '.join(txt.lower().split())


def process_rows(jobid, from_id, to_id, source):
    logger = Logger()

    if source == 'adzuna':
        table = ADZJob
    elif source == 'indeedjob':
        table = IndeedJob

    filters = [table.id >= from_id]
    if to_id is not None:
        filters.append(table.id < to_id)

    if source == 'indeedjob':
        psdb = ParseDB()

    with DatoinDB() as dtdb:
        q = dtdb.query(table).filter(*filters)

        def process_row(row):
            if source == 'adzuna':
                text = row.full_description
            elif source == 'indeedjob':
                result = psdb.query(ParseINJob.description).filter(ParseINJob.jobkey == row.jobkey).first()
                text = result[0] if result is not None else row.snippet

            d = {'source': source,
                 'parent_id': row.id,
                 #'location1': row.location1,
                 'text': sanitize(text)}
            dtdb.add_duplicate_job(d)

        process_db(q, process_row, dtdb, logger=logger)


def main(args):
    logger = Logger()

    if args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = IndeedJob

    with DatoinDB() as dtdb:
        filters = [table.id > args.from_id]
        q = dtdb.query(table.id).filter(*filters)

        split_process(q, process_rows, args.batch_size,
                      njobs=args.jobs, args=[args.source], logger=logger,
                      workdir='jobs', prefix='pre_dup')

    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', choices=['adzuna', 'indeedjob'],
                        help='The data source to process.')
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--from-id', type=int, default=1,
                        help='Start of timestamp range.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    args = parser.parse_args()
    main(args)
