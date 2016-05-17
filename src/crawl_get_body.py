import sys
sys.path.append('../src')

import conf
from crawldb import *
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('id', type=int,
                        help='ID of the record to get.')
    args = parser.parse_args()

    crdb = CrawlDB(conf.CRAWL_DB)

    q = crdb.query(LIProfile.body) \
            .filter(LIProfile.id == args.id)
    for body, in q:
        sys.stdout.write(body)
        sys.stdout.flush()
