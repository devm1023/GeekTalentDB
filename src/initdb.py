import conf
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('database',
                    choices=['datoin', 'canonical', 'analytics', 'geekmaps',
                             'careerhacker'],
                    help='The database to initialize.')
parser.add_argument('--no-create', action='store_true',
                    help='Do not create new tables.')
parser.add_argument('--no-delete', action='store_true',
                    help='Do not delete existing tables.')
args = parser.parse_args()

nocreate = args.no_create
nodelete = args.no_delete
    
if args.database == 'datoin':
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
elif args.database == 'careerhacker':
    from careerhackerdb import CareerHackerDB
    chdb = CareerHackerDB(url=conf.CAREERHACKER_DB)
    if not nodelete:
        chdb.drop_all()
    if not nocreate:
        chdb.create_all()
