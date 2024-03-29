import requests
import argparse
from fuzzywuzzy import fuzz
from sqlalchemy import and_
from sqlalchemy.sql.expression import literal_column
import csv
import conf
import urllib.parse as url
from datoindb import *
from dbtools import dict_from_row
import collections
from html.parser import HTMLParser
from windowquery import split_process, process_db
from logger import Logger


"""
    This script deals with Adzuna duplicates within itself.
    Script calculates difference ratios and outputs those to a CSV file. 
"""

class MLStripper(HTMLParser):
    """Strips HTML from strings """
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def process_rows(jobid, from_id, to_id, threshold, title_threshold, job_posts):
    logger = Logger()

    if args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = IndeedJob

    filters = [table.id >= from_id]
    if to_id is not None:
        filters.append(table.id < to_id)

    with DatoinDB() as dtdb:
        if args.source == 'adzuna':
            q = dtdb.query(ADZJob.id, ADZJob.full_description, Duplicates.text, ADZJob.title, ADZJob.location1)
        elif args.source == 'indeedjob':
            null_column = literal_column("NULL")
            q = dtdb.query(IndeedJob.id, null_column, Duplicates.text, IndeedJob.jobtitle, null_column)

        q = q.join(Duplicates, and_(Duplicates.parent_id == table.id, Duplicates.source == args.source)) \
             .filter(*filters)

        with open('{}.{}'.format(args.out_file, from_id), 'w') as outputfile:
            csvwriter = csv.writer(outputfile)

            def process_row(row):

                def compare(txt1, txt2):
                    return fuzz.ratio(txt1, txt2)

                row_id, full_description_1, str1, title_1, location1_1 = row

                str1 = ' '.join(sorted(str1.split()))
                title_1 = ' '.join(sorted(title_1.split()))

                compared_count = 0

                if row_id % 10 == 0:
                    print('{0} rows checked.'.format(row_id))
                for key, val in job_posts.items():
                    if key % 1000 == 0:
                        print('Outer:{0}, Inner: {1} working rows. (Compared: {2})'.format(row_id, key, compared_count))

                    full_description_2, str2, title_2, location1_2 = val
                    if key > row_id or args.source != args.reference:
                        if location1_2 != location1_1 and location1_2 is not None:
                            continue

                        if compare(title_1, title_2) <= title_threshold:
                            continue

                        len_ratio = max(len(str1), len(str2)) / min(len(str1), len(str2))

                        # differing lengths
                        if len_ratio > 2.0:
                            continue

                        compared_count += 1
                        delta = compare(str1, str2)
                        if delta > threshold:
                            print('Potential duplicates: {0} and {1} score: {2}\ntext1: {3}\ntext2: {4}'
                                    .format(row_id, key, delta, full_description_1, full_description_2))
                            csvwriter.writerow([row_id, key, delta, full_description_1, full_description_2])


            process_db(q, process_row, dtdb, logger=logger)


def main(args):
    logger = Logger()

    threshold = args.limit
    title_threshold = args.title_limit

    with DatoinDB() as dtdb:
        print('Getting job post data...')

        if args.reference == 'adzuna':
            ref_table = ADZJob
            q = dtdb.query(ADZJob.id, ADZJob.full_description, Duplicates.text, ADZJob.title, ADZJob.location1)
        elif args.reference == 'indeedjob':
            ref_table = IndeedJob
            null_column = literal_column("NULL")
            q = dtdb.query(IndeedJob.id, null_column, Duplicates.text, IndeedJob.jobtitle, null_column)

        q = q.join(Duplicates, and_(Duplicates.parent_id == ref_table.id, Duplicates.source == args.reference)) \
             .order_by(ref_table.id)

        job_posts = dict(tuple())
        for row in q:
            id = row[0]
            sorted_text = ' '.join(sorted(row[2].split()))
            sorted_title = ' '.join(sorted(row[3].split()))

            job_posts[id] = (row[1], sorted_text, sorted_title, *row[4:])

        print('Processing...')

        if args.source == 'adzuna':
            table = ADZJob
        elif args.source == 'indeedjob':
            table = IndeedJob

        filters = [table.id > args.from_id]
        q = dtdb.query(table.id).filter(*filters)

        split_process(q, process_rows, args.batch_size, args=[threshold, title_threshold, job_posts],
                      njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='de_dup')

    # with open(args.out_file, 'w') as outputfile:
    #     csvwriter = csv.writer(outputfile)
    #     for row1 in q1:
    #         str1 = strip_tags(row1.full_description)
    #         str1 = ' '.join(str1.split())
    #         if row1.id % 10 == 0:
    #             print('{0} checked.'.format(row1.id))
    #         for row2 in q2:
    #             if row2.id % 1000 == 0:
    #                 print('Outer:{0}, Inner: {1} working rows.'.format(row1.id, row2.id))
    #             if row2.id > row1.id:
    #                 if row1.location1 == row2.location1 or row2.location1 is None:
    #                     str2 = strip_tags(row2.full_description)
    #                     str2 = ' '.join(str2.split())
    #                     delta = compare(str1, str2)
    #                     if delta > threshold:
    #                         print('Potential duplicates: {0} and {1} score: {2}\ntext1: {3}\ntext2: {4}'
    #                               .format(row1.id, row2.id, delta, row1.full_description, row2.full_description))
    #                         csvwriter.writerow([row1.id, row2.id, delta, row1.full_description, row2.full_description])
    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', choices=['adzuna', 'indeedjob'],
                        help='The data source to process.')
    parser.add_argument('--reference', choices=['adzuna', 'indeedjob'],
                        help='The data source to compare against.')
    parser.add_argument('--out_file', type=str,
                        help='CSV files where output will be written to.', default=None, required=True)
    parser.add_argument('--limit', type=int,
                        help='Threshold for a match to be significant. (0 - 100)', default=75)
    parser.add_argument('--title-limit', type=int,
                        help='Threshold for a title to be significant. (0 - 100)', default=40)
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--from-id', type=int, default=1,
                        help='Start of id range.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    args = parser.parse_args()
    main(args)
