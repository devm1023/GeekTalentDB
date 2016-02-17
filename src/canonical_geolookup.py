import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import splitProcess, processDb
from datetime import datetime, timedelta
import time
import argparse


SOURCES = ['linkedin', 'indeed']

def processLocations(jobid, fromlocation, tolocation,
                     fromdate, todate, byIndexedOn, source, retry, maxretry):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    if source == 'linkedin':
        profileTab = LIProfile
        experienceTab = LIExperience
    elif source == 'indeed':
        profileTab = INProfile
        experienceTab = INExperience
    else:
        raise ValueError('Invalid source type.')

    if retry:
        q = cndb.query(Location.nrmName) \
                .filter(Location.placeId == None,
                        Location.tries < maxretry)
        if fromlocation is not None:
            q = q.filter(Location.nrmName >= fromlocation)
    else:
        if byIndexedOn:
            q1 = cndb.query(profileTab.nrmLocation.label('nrmloc')) \
                     .filter(profileTab.indexedOn >= fromdate,
                             profileTab.indexedOn < todate)
            q2 = cndb.query(experienceTab.nrmLocation.label('nrmloc')) \
                     .join(profileTab) \
                     .filter(profileTab.indexedOn >= fromdate,
                             profileTab.indexedOn < todate)
        else:
            q1 = cndb.query(profileTab.nrmLocation.label('nrmloc')) \
                     .filter(profileTab.crawledOn >= fromdate,
                             profileTab.crawledOn < todate)
            q2 = cndb.query(experienceTab.nrmLocation.label('nrmloc')) \
                     .join(profileTab) \
                     .filter(profileTab.crawledOn >= fromdate,
                             profileTab.crawledOn < todate)

        q1 = q1.filter(profileTab.nrmLocation >= fromlocation)
        q2 = q2.filter(experienceTab.nrmLocation >= fromlocation)
        if tolocation is not None:
            q1 = q1.filter(profileTab.nrmLocation < tolocation)
            q2 = q2.filter(experienceTab.nrmLocation < tolocation)

        q = q1.union(q2)

    def addLocation(rec):
        cndb.addLocation(rec[0], retry=retry, logger=logger)
 
    processDb(q, addLocation, cndb, logger=logger)

def run(args, maxretry):
    logger = Logger(sys.stdout)
    if args.source is None:
        logger.log('Processing LinkedIn locations.\n')
        args.source = 'linkedin'
        run(args, maxretry)
        logger.log('Processing Indeed locations.\n')
        args.source = 'indeed'
        run(args, maxretry)
        return
    elif args.source == 'linkedin':
        profiletab = LIProfile
        experiencetab = LIExperience
    elif args.source == 'indeed':
        profiletab = INProfile
        experiencetab = INExperience
    else:
        raise ValueError('Invalid source.')
    
    cndb = CanonicalDB(url=conf.CANONICAL_DB)

    if args.retry:
        q = cndb.query(Location.nrmName) \
                .filter(Location.placeId == None,
                        Location.tries < maxretry)
        if fromlocation is not None:
            q = q.filter(Location.nrmName >= args.from_location)
    else:
        if args.by_index_date:
            q1 = cndb.query(profiletab.nrmLocation.label('nrmloc')) \
                     .filter(profiletab.indexedOn >= args.from_date,
                             profiletab.indexedOn < args.to_date)
            q2 = cndb.query(experiencetab.nrmLocation.label('nrmloc')) \
                     .join(profiletab) \
                     .filter(profiletab.indexedOn >= args.from_date,
                             profiletab.indexedOn < args.to_date)
        else:
            q1 = cndb.query(profiletab.nrmLocation.label('nrmloc')) \
                     .filter(profiletab.crawledOn >= args.from_date,
                             profiletab.crawledOn < args.to_date)
            q2 = cndb.query(experiencetab.nrmLocation.label('nrmloc')) \
                     .join(profiletab) \
                     .filter(profiletab.crawledOn >= args.from_date,
                             profiletab.crawledOn < args.to_date)

        if args.from_location is not None:
            q1 = q1.filter(profiletab.nrmLocation >= args.from_location)
            q2 = q2.filter(experiencetab.nrmLocation >= args.from_location)

        q = q1.union(q2)

    splitProcess(q, processLocations, args.batchsize, njobs=args.njobs,
                 args=[args.from_date, args.to_date, args.by_index_date,
                       args.source, args.retry, maxretry],
                 logger=logger, workdir='jobs', prefix='canonical_geoupdate')
    


parser = argparse.ArgumentParser()
parser.add_argument('njobs', help='Number of parallel jobs.', type=int)
parser.add_argument('batchsize', help='Number of rows per batch.', type=int)
parser.add_argument('--from-date', help=
                    'Only process profiles crawled or indexed on or after\n'
                    'this date. Format: YYYY-MM-DD',
                    default='1970-01-01')
parser.add_argument('--to-date', help=
                    'Only process profiles crawled or indexed before\n'
                    'this date. Format: YYYY-MM-DD')
parser.add_argument('--by-index-date', help=
                    'Indicates that the dates specified with --fromdate and\n'
                    '--todate are index dates. Otherwise they are interpreted\n'
                    'as crawl dates.',
                    action='store_true')
parser.add_argument('--from-location', help=
                    'Start processing from this location name. Useful for\n'
                    'crash recovery.')
parser.add_argument('--retry', type=int, help=
                    'The number of times failed location lookups should be\n'
                    're-tried. If specified the failed lookups in the\n'
                    'existing locations table are re-tried. Do not specify if\n'
                    'you want to add new locations to the table.')
parser.add_argument('--source', choices=['linkedin', 'indeed'], help=
                    'Source type to process. If not specified all sources are\n'
                    'processed.')
args = parser.parse_args()

if not args.retry:
    maxretry = 0
    args.retry = False
else:
    maxretry = args.retry
    args.retry = True

try:
    args.from_date = datetime.strptime(args.from_date, '%Y-%m-%d')
    if not args.to_date:
        args.to_date = datetime.now()
    else:
        args.to_date = datetime.strptime(args.to_date, '%Y-%m-%d')
except ValueError:
    sys.stderr.write('Error: Invalid date.\n')
    exit(1)

run(args, maxretry)


