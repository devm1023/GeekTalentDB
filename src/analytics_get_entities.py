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
    entities = andb.find_entities(args.type,
                                  args.source,
                                  args.language,
                                  args.query)
    entities.sort(key=lambda x: x[2])
    print('num profiles  num sub-docs  entity')
    for nrm_name, name, profile_count, sub_document_count in entities:
        print('{1:> 12d}  {2:> 12d}  {0:s}' \
              .format(name, profile_count, sub_document_count))
