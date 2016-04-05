import conf
from canonicaldb import *
import sys
from datetime import datetime, timedelta
from logger import Logger
from sqlalchemy import and_
from windowquery import split_process, process_db
from datetime import datetime, timedelta
import time
import argparse


SOURCES = ['linkedin', 'indeed']

def process_locations(jobid, fromlocation, tolocation,
                     fromdate, todate, by_indexed_on, source, retry, maxretry):
    cndb = CanonicalDB(url=conf.CANONICAL_DB)
    logger = Logger(sys.stdout)

    if source == 'linkedin':
        profile_tab = LIProfile
        experience_tab = LIExperience
    elif source == 'indeed':
        profile_tab = INProfile
        experience_tab = INExperience
    else:
        raise ValueError('Invalid source type.')

    if retry:
        q = cndb.query(Location.nrm_name) \
                .filter(Location.place_id == None,
                        Location.tries < maxretry)
        if fromlocation is not None:
            q = q.filter(Location.nrm_name >= fromlocation)
    else:
        if by_indexed_on:
            q1 = cndb.query(profile_tab.nrm_location.label('nrmloc')) \
                     .filter(profile_tab.indexed_on >= fromdate,
                             profile_tab.indexed_on < todate)
            q2 = cndb.query(experience_tab.nrm_location.label('nrmloc')) \
                     .join(profile_tab) \
                     .filter(profile_tab.indexed_on >= fromdate,
                             profile_tab.indexed_on < todate)
        else:
            q1 = cndb.query(profile_tab.nrm_location.label('nrmloc')) \
                     .filter(profile_tab.crawled_on >= fromdate,
                             profile_tab.crawled_on < todate)
            q2 = cndb.query(experience_tab.nrm_location.label('nrmloc')) \
                     .join(profile_tab) \
                     .filter(profile_tab.crawled_on >= fromdate,
                             profile_tab.crawled_on < todate)

        q1 = q1.filter(profile_tab.nrm_location >= fromlocation)
        q2 = q2.filter(experience_tab.nrm_location >= fromlocation)
        if tolocation is not None:
            q1 = q1.filter(profile_tab.nrm_location < tolocation)
            q2 = q2.filter(experience_tab.nrm_location < tolocation)

        q = q1.union(q2)

    def add_location(rec):
        cndb.add_location(rec[0], retry=retry, logger=logger)

    process_db(q, add_location, cndb, logger=logger)

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
        q = cndb.query(Location.nrm_name) \
                .filter(Location.place_id == None,
                        Location.tries < maxretry)
        if fromlocation is not None:
            q = q.filter(Location.nrm_name >= args.from_location)
    else:
        if args.by_index_date:
            q1 = cndb.query(profiletab.nrm_location.label('nrmloc')) \
                     .filter(profiletab.indexed_on >= args.from_date,
                             profiletab.indexed_on < args.to_date)
            q2 = cndb.query(experiencetab.nrm_location.label('nrmloc')) \
                     .join(profiletab) \
                     .filter(profiletab.indexed_on >= args.from_date,
                             profiletab.indexed_on < args.to_date)
        else:
            q1 = cndb.query(profiletab.nrm_location.label('nrmloc')) \
                     .filter(profiletab.crawled_on >= args.from_date,
                             profiletab.crawled_on < args.to_date)
            q2 = cndb.query(experiencetab.nrm_location.label('nrmloc')) \
                     .join(profiletab) \
                     .filter(profiletab.crawled_on >= args.from_date,
                             profiletab.crawled_on < args.to_date)

        if args.from_location is not None:
            q1 = q1.filter(profiletab.nrm_location >= args.from_location)
            q2 = q2.filter(experiencetab.nrm_location >= args.from_location)

        q = q1.union(q2)

    split_process(q, process_locations, args.batchsize, njobs=args.jobs,
                 args=[args.from_date, args.to_date, args.by_index_date,
                       args.source, args.retry, maxretry],
                 logger=logger, workdir='jobs', prefix='canonical_geoupdate')



parser = argparse.ArgumentParser()
parser.add_argument('--jobs', type=int, default=1,
                    help='Number of parallel jobs.')
parser.add_argument('--batchsize', type=int, default=1000,
                    help='Number of rows per batch.')
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


