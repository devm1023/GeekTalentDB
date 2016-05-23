import sys

import conf
from crawldb import *
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('id', type=int,
                        help='ID of the record to get.')
    args = parser.parse_args()

    crdb = CrawlDB(conf.CRAWL_DB)

    q = crdb.query(Website.html) \
            .filter(Website.id == args.id)
    for html, in q:
        sys.stdout.write(html)
        sys.stdout.flush()
