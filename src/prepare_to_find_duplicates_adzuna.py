import requests
import argparse
from fuzzywuzzy import fuzz
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
    Script prepares a copy of full description field for each job which will be used for identifying duplicates.
    Following formatting is applied to full description text: html strip, whitespace normalisation, lowercasing.
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


def sanitize(txt):
    txt = strip_tags(txt)
    return ' '.join(txt.split())


def process_rows(jobid, from_id, to_id):
    logger = Logger()
    filters = [ADZJob.id >= from_id]
    if to_id is not None:
        filters.append(ADZJob.id < to_id)

    with DatoinDB() as dtdb:
        q = dtdb.query(ADZJob).filter(*filters)

        # # construct query that retreives the latest version of each profile
        # maxts = func.max(Webpage.timestamp) \
        #     .over(partition_by=Webpage.redirect_url) \
        #     .label('maxts')
        # subq = crdb.query(Webpage, maxts) \
        #     .filter(*filters) \
        #     .subquery()
        # WebpageAlias = aliased(Webpage, subq)
        # if from_ts is not None:
        #     q = q.filter(subq.c.maxts >= from_ts)
        # if to_ts is not None:
        #     q = q.filter(subq.c.maxts < to_ts)

        def process_row(row):
            d = {'source': args.source,
                 'parent_id': row.id,
                 'location1': row.location1,
                 'text': sanitize(row.full_description)}
            dtdb.add_duplicate_job(d)

        # this function does the parsing
        # def process_row(webpage):
        #     try:
        #         doc = parse_html(webpage.html)
        #     except:
        #         return
        #     try:
        #         parsed_profile = parse_profile(
        #             webpage.url, webpage.redirect_url, webpage.timestamp, doc)
        #     except:
        #         logger.log('Error parsing HTML from URL {0:s}\n' \
        #                    .format(webpage.url))
        #         raise
        #     if parsed_profile is not None:
        #         psdb.add_from_dict(parsed_profile, LIProfile)

        # apply process_row to each row returned by q and commit to psdb
        # in regular intervals
        process_db(q, process_row, dtdb, logger=logger)


def main(args):
    logger = Logger()
    dtdb = DatoinDB()


    with DatoinDB() as dtdb:
        filters = [ADZJob.id > args.from_id]
        q = dtdb.query(ADZJob.id).filter(*filters)

        split_process(q, process_rows, args.batch_size,
                      njobs=args.jobs, logger=logger,
                      workdir='jobs', prefix='pre_dup')

    print('Done!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=str,
                        help='CSV files where output will be written to.', default=None, required=True)
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--from-id', type=int, default=1,
                        help='Start of timestamp range.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    args = parser.parse_args()
    main(args)
