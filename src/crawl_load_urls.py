from crawldb import *
from logger import Logger
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', default=None,
                        help='Type of the URLs. Default: None')
    parser.add_argument('site',
                        help='ID string of the crawled site.')
    parser.add_argument('input_file',
                        help='The file holding the URLs to add.')
    args = parser.parse_args()

    logger = Logger()
    batch_size = 10000

    with CrawlDB() as crdb, open(args.input_file, 'r') as inputfile:
        count = 0
        for line in inputfile:
            count += 1
            url = line.strip()
            crdb.add_url(args.site, args.type, url)
            if count % batch_size == 0:
                crdb.commit()
                logger.log('{0:d} URLs loaded.\n'.format(count))
        if count % batch_size != 0:
            logger.log('{0:d} URLs loaded.\n'.format(count))
        crdb.commit()



