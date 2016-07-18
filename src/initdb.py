import conf
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('database',
                    choices=['crawl', 'parse', 'datoin', 'canonical',
                             'analytics', 'geekmaps', 'careerdefinition',
                             'description', 'watson'],
                    help='The database to initialize.')
parser.add_argument('--no-create', action='store_true',
                    help='Do not create new tables.')
parser.add_argument('--no-delete', action='store_true',
                    help='Do not delete existing tables.')
args = parser.parse_args()

nocreate = args.no_create
nodelete = args.no_delete

if args.database == 'crawl':
    from crawldb import CrawlDB
    crdb = CrawlDB(url=conf.CRAWL_DB)
    if not nodelete:
        crdb.drop_all()
    if not nocreate:
        crdb.create_all()
if args.database == 'parse':
    from parsedb import ParseDB
    padb = ParseDB(url=conf.PARSE_DB)
    if not nodelete:
        padb.drop_all()
    if not nocreate:
        padb.create_all()
elif args.database == 'datoin':
    from datoindb import DatoinDB
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    if not nodelete:
        dtdb.drop_all()
    if not nocreate:
        dtdb.create_all()
elif args.database == 'canonical':
    from canonicaldb import CanonicalDB
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    if not nodelete:
        cndb.drop_all()
    if not nocreate:
        cndb.create_all()
elif args.database == 'analytics':
    from analyticsdb import AnalyticsDB
    andb = AnalyticsDB(url=conf.ANALYTICS_DB)
    if not nodelete:
        andb.drop_all()
    if not nocreate:
        andb.create_all()
elif args.database == 'geekmaps':
    from geekmapsdb import GeekMapsDB
    gmdb = GeekMapsDB(url=conf.GEEKMAPS_DB)
    if not nodelete:
        gmdb.drop_all()
    if not nocreate:
        gmdb.create_all()
elif args.database == 'careerdefinition':
    from careerdefinitiondb import CareerDefinitionDB
    cddb = CareerDefinitionDB(url=conf.CAREERDEFINITION_DB)
    if not nodelete:
        cddb.drop_all()
    if not nocreate:
        cddb.create_all()
elif args.database == 'description':
    from descriptiondb import DescriptionDB
    dscdb = DescriptionDB(url=conf.DESCRIPTION_DB)
    if not nodelete:
        dscdb.drop_all()
    if not nocreate:
        dscdb.create_all()
elif args.database == 'watson':
    from watsondb import WatsonDB
    dscdb = WatsonDB(url=conf.WATSON_DB)
    if not nodelete:
        dscdb.drop_all()
    if not nocreate:
        dscdb.create_all()
