import argparse

parser = argparse.ArgumentParser()
parser.add_argument('database',
                    choices=['crawl', 'parse', 'datoin', 'canonical',
                             'careerdefinition', 'description', 'watson'],
                    help='The database to initialize.')
parser.add_argument('--no-create', action='store_true',
                    help='Do not create new tables.')
parser.add_argument('--no-delete', action='store_true',
                    help='Do not delete existing tables.')
args = parser.parse_args()

nocreate = args.no_create
nodelete = args.no_delete

if args.database == 'crawl':
    import crawldb as db
if args.database == 'parse':
    import parsedb as db
elif args.database == 'datoin':
    import datoindb as db
elif args.database == 'canonical':
    import canonicaldb as db
elif args.database == 'careerdefinition':
    import careerdefinitiondb as db
elif args.database == 'description':
    import descriptiondb as db
elif args.database == 'watson':
    import watsondb as db

if not nodelete:
    db.drop_all()
if not nocreate:
    db.create_all()
