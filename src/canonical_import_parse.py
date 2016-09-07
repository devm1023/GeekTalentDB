# This script it takes the data from 
# parseDb and inserts it into canonical
# It adds the data into canonicalDb without dropping the database
# so additional profiles can be added into a current database
# this does not include geo/word processing
from parsedb import *
from canonicaldb import CanonicalDB
from windowquery import split_process, process_db
from datetime import datetime
from logger import Logger
from parse_datetime import parse_datetime
import re
import hashlib
import argparse

connectionspatt = re.compile(r'[^0-9]*([0-9]+)[^0-9]*')
country_languages = {
    'United Kingdom' : 'en',
    'Netherlands'    : 'nl',
    'Nederland'      : 'nl',
}


def make_connections(connections):
    if connections is not None:
        connections = connectionspatt.match(connections)
    if connections is not None:
        connections = int(connections.group(1))
    return connections


def make_date(datestr):
    if not datestr:
        return None
    datestr = ' '.join(datestr.split())
    date = None
    try:
        date = datetime.strptime('%B %Y')
    except:
        pass
    if not date:
        try:
            date = datetime.strptime('%Y')
        except:
            pass

    return date


def make_hash(url):
    hasher = hashlib.md5()
    hasher.update(url.encode('utf-8'))
    digest = hasher.hexdigest()
    return 'linkedin:inhouse:'+digest


def import_liprofiles(jobid, fromid, toid, from_ts, to_ts):
    logger = Logger()
    psdb = ParseDB()
    cndb = CanonicalDB()

    q = psdb.query(LIProfile).filter(LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)
    if from_ts is not None:
        q = q.filter(LIProfile.timestamp >= from_ts)
    if to_ts is not None:
        q = q.filter(LIProfile.timestamp < to_ts)

    def add_liprofile(liprofile):
        profiledict = dict(
            datoin_id     = make_hash(liprofile.url),
            indexed_on    = None,
            crawled_on    = liprofile.timestamp,
            crawl_fail_count = 0,
            language      = 'en',  # for now, assume all profiles are english
            url           = liprofile.url,
            picture_url   = liprofile.picture_url,
            name          = liprofile.name,
            last_name     = None,
            first_name    = None,
            location      = liprofile.location,
            sector        = liprofile.sector,
            title         = liprofile.title,
            description   = liprofile.description,
            connections   = make_connections(liprofile.connections),

            skills        = [s.name for s in liprofile.skills if s.name],
            experiences   = [],
            educations    = [],
            groups        = [],
        )

        profiledict['skills'] = list(set(profiledict['skills']))

        for experience in liprofile.experiences:
            experiencedict = dict(
                title        = experience.title,
                company      = experience.company,
                location     = experience.location,
                start        = make_date(experience.start),
                end          = make_date(experience.end),
                description  = experience.description,
            )
            if not any(bool(experiencedict[k]) for k in \
                       ['title', 'company', 'location', 'description']):
                continue
            profiledict['experiences'].append(experiencedict)

        for education in liprofile.educations:
            educationdict = dict(
                institute   = education.institute,
                degree      = None,
                subject     = education.course,
                start       = make_date(education.start),
                end         = make_date(education.end),
                description = education.description,
            )
            if not any(bool(educationdict[k]) for k in \
                       ['institute', 'subject', 'description']):
                continue
            profiledict['educations'].append(educationdict)

        for group in liprofile.groups:
            groupdict = dict(
                name = group.name,
                url  = group.url,
            )
            if not any(bool(groupdict[k]) for k in ['name', 'url']):
                continue
            profiledict['groups'].append(groupdict)

        cndb.add_liprofile(profiledict)

    process_db(q, add_liprofile, cndb, logger=logger)


def main(args):
    njobs = max(args.jobs, 1)
    batchsize = args.batch_size
    from_ts = parse_datetime(args.from_timestamp)
    to_ts = parse_datetime(args.to_timestamp)

    psdb = ParseDB()
    logger = Logger()

    query = psdb.query(LIProfile.id)
    if from_ts:
        query = query.filter(LIProfile.timestamp >= from_ts)
    if to_ts:
        query = query.filter(LIProfile.timestamp < to_ts)
    if args.from_id is not None:
        query = query.filter(table.id >= from_id)

    split_process(query, import_liprofiles, args.batch_size,
                  njobs=njobs, args=[from_ts, to_ts],
                  logger=logger, workdir='jobs',
                  prefix='canonical_import_parse')
    
    
if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-timestamp', help=
                        'Only process profiles crawled or indexed on or after '
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-timestamp', help=
                        'Only process profiles crawled or indexed before '
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--from-id', help=
                        'Start processing from this datoin ID. Useful for '
                        'crash recovery.')
    args = parser.parse_args()
    main(args)
