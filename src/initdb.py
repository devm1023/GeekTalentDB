import argparse

parser = argparse.ArgumentParser()
parser.add_argument('database',
                    choices=['crawl', 'parse', 'datoin', 'canonical',
                             'careerdefinition', 'description', 'watson', 'whichuni'],
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
    db = CrawlDB()
if args.database == 'parse':
    from parsedb import ParseDB
    db = ParseDB()
elif args.database == 'datoin':
    from datoindb import DatoinDB
    db = DatoinDB()
elif args.database == 'canonical':
    from canonicaldb import CanonicalDB
    db = CanonicalDB()
elif args.database == 'careerdefinition':
    from careerdefinitiondb import CareerDefinitionDB
    db = CareerDefinitionDB()
elif args.database == 'description':
    from descriptiondb import DescriptionDB
    db = DescriptionDB()
elif args.database == 'watson':
    from watsondb import WatsonDB
    db = WatsonDB()
elif args.database == 'whichuni':
    from whichunidb import WhichUniDB
    db = WhichUniDB()

if not nodelete:
    db.drop_all()
if not nocreate:
    db.create_all()
