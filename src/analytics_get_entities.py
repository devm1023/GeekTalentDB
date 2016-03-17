import conf
from analyticsdb import *
from logger import Logger
import sys
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('type',
                        help='The query type.',
                        choices=['skill', 'title', 'company',
                                 'sector', 'institute', 'subject', 'degree'])
    parser.add_argument('source',
                        help='The data source to use.',
                        choices=['linkedin', 'indeed'])
    parser.add_argument('language',
                        help='The query language.',
                        choices=['en', 'nl'])
    parser.add_argument('query',
                        help='The search term.')
    args = parser.parse_args()
    
    andb = AnalyticsDB(conf.ANALYTICS_DB)
    entities = andb.findEntities(args.type,
                                 args.source,
                                 args.language,
                                 args.query)
    for nrmName, name, profileCount, subDocumentCount in reversed(entities):
        print('{0:s} ({1:d} profiles, {2:d} sub-documents)' \
              .format(name, profileCount, subDocumentCount))
