import argparse
from shapely.geometry import Point

from canonicaldb import *
import conf
from logger import Logger
from nuts import NutsRegions
from windowquery import split_process, process_db

def map_job_nuts(jobid, fromid, toid):
    logger = Logger()
    cndb = CanonicalDB()

    countries = [
        'AT', 'BE', 'BG', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'EL', 'ES', 'FI', 'FR', 'HR',
        'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'ME', 'MK', 'MT', 'NL', 'NO', 'PL',
        'PT', 'RO', 'SE', 'SI', 'SK', 'TR', 'UK'
    ]

    nuts = NutsRegions(conf.NUTS_DATA, countries=countries)

    if args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = INJob

    q = cndb.query(table).filter(table.id >= fromid)

    if toid is not None:
        q = q.filter(table.id < toid)

    if args.sector is not None:
        q = q.filter(table.category == args.sector)

    def map_nuts(job):
        if not job.longitude:
            return
        (job.nuts0, job.nuts1,
            job.nuts2, job.nuts3) = nuts.get_ids(Point(job.longitude, job.latitude))

    process_db(q, map_nuts, cndb, logger=logger)


def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size

    cndb = CanonicalDB()
    logger = Logger()

    if args.source == 'adzuna':
        table = ADZJob
    elif args.source == 'indeedjob':
        table = INJob

    query = cndb.query(table.id)
    if args.from_id is not None:
        query = query.filter(table.id >= args.from_id)

    if args.sector is not None:
        query = query.filter(table.category == args.sector)

    split_process(query, map_job_nuts, args.batch_size,
                njobs=njobs, logger=logger, workdir='jobs',
                prefix='canonical_job_nuts')


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-id', help=
                        'Start processing from this ID. Useful for '
                        'crash recovery.')
    parser.add_argument('--sector')
    parser.add_argument('--source',
                    choices=['adzuna', 'indeedjob'],
                    help=
                    'Source type to process.')
    args = parser.parse_args()

    main(args)
