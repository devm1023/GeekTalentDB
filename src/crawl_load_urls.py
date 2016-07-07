import conf
from crawldb import *
from logger import Logger
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--leafs', action='store_true',
                        help='Mark URLs as leaf pages.')
    parser.add_argument('--level', type=int, default=None,
                        help='Level of the URLs. Default: None')
    parser.add_argument('site',
                        help='ID string of the crawled site.')
    parser.add_argument('input_file',
                        help='The file holding the URLs to add.')
    args = parser.parse_args()

    crdb = CrawlDB(conf.CRAWL_DB)
    logger = Logger()
    
    crdb.load_urls(args.site, args.leafs, args.level, args.input_file,
                   logger=logger)
    
