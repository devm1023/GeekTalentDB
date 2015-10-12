from datoindb import *
import normalformdb as nf
from windowquery import windowQuery, windows
from sqlalchemy import and_
import conf
import sys
from datetime import datetime, timedelta
from logger import Logger
from parallelize import ParallelFunction

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()


def parseProfiles(fromTs, toTs, fromid, toid, offset):
    batchsize = 100
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DT_READ_DB)
    nfdb = nf.NormalFormDB(url=conf.NF_WRITE_DB)

    q = dtdb.query(LIProfile).filter(LIProfile.indexedOn >= fromTs,
                                     LIProfile.indexedOn < toTs,
                                     LIProfile.id >= fromid)
    if toid is not None:
        q = q.filter(LIProfile.id < toid)

    profilecount = 0
    for liprofile in q:
        profilecount += 1
        
        if liprofile.name:
            name = liprofile.name
        elif liprofile.firstName and liprofile.lastName:
            name = ' '.join([liprofile.firstName, liprofile.lastName])
        elif liprofile.lastName:
            name = liprofile.lastName
        elif liprofile.firstName:
            name = liprofile.firstName
        else:
            continue

        if liprofile.city and liprofile.country:
            location = ', '.join([liprofile.city, liprofile.country])
        elif liprofile.country:
            location = liprofile.country
        elif liprofile.city:
            location = liprofile.city
        else:
            location = None

        if liprofile.indexedOn:
            indexedOn = timestamp0 + timedelta(milliseconds=liprofile.indexedOn)
        else:
            indexedOn = None
        
        profiledict = {
            'datoinId'    : liprofile.id,
            'name'        : name,
            'location'    : location,
            'title'       : liprofile.title,
            'description' : liprofile.description,
            'url'         : liprofile.profileUrl,
            'pictureUrl'  : liprofile.profilePictureUrl,
            'skills'      : liprofile.categories,
            'indexedOn'   : indexedOn,
        }

        experiencedicts = []
        for experience in dtdb.query(Experience) \
                              .filter(Experience.parentId == liprofile.id):
            if experience.dateFrom:
                start = timestamp0 + timedelta(milliseconds=experience.dateFrom)
            else:
                start = None
            if start is not None and experience.dateTo:
                end = timestamp0 + timedelta(milliseconds=experience.dateTo)
            else:
                end = None
            if start and end and start > end:
                start = None
                end = None

            if experience.indexedOn:
                indexedOn = timestamp0 + \
                            timedelta(milliseconds=experience.indexedOn)
            else:
                indexedOn = None

            experiencedict = {
                'datoinId'       : experience.id,
                'title'          : experience.name,
                'company'        : experience.company,
                'start'          : start,
                'end'            : end,
                'description'    : experience.description,
                'indexedOn'      : indexedOn,
                }
            experiencedicts.append(experiencedict)

        nfdb.addLIProfile(profiledict, experiencedicts, [], now)

        if profilecount % batchsize == 0:
            nfdb.commit()
            logger.log('{0:d} profiles processed.\n' \
                       .format(profilecount+offset))
            logger.log('Last profile id: {0:s}\n'.format(liprofile.id))

    # final commit
    if profilecount % batchsize != 0:
        nfdb.commit()
        logger.log('{0:d} profiles processed.\n' \
                   .format(profilecount+offset))

    return profilecount, liprofile.id



# process arguments

njobs = int(sys.argv[1])
batchsize = int(sys.argv[2])
fromdate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[4], '%Y-%m-%d')
fromid = None
if len(sys.argv) > 5:
    fromid = sys.argv[5]

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000

filter = and_(LIProfile.indexedOn >= fromTs, LIProfile.indexedOn < toTs)
if fromid is not None:
    filter = and_(filter, LIProfile.id > fromid)

dtdb = DatoinDB(url=conf.DT_READ_DB)
logger = Logger(sys.stdout)

if njobs <= 1:
    profilecount = 0
    for fromid, toid in windows(dtdb.session, LIProfile.id, batchsize, filter):
        nprofiles, lastid = parseProfiles(fromTs, toTs, fromid, toid,
                                          profilecount)
        logger.log('Last profile id: {0:s}\n'.format(lastid))
        profilecount += nprofiles
else:
    args = []
    parallelParse = ParallelFunction(parseProfiles,
                                     batchsize=1,
                                     workdir='parsejobs',
                                     prefix='linormalize',
                                     tries=1)
    profilecount = 0
    for fromid, toid in windows(dtdb.session, LIProfile.id, batchsize, filter):
        args.append((fromTs, toTs, fromid, toid, 0))
        if len(args) == njobs:
            starttime = datetime.now()
            logger.log('Starting batch at {0:s}.\n' \
                       .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
            results = parallelParse(args)
            endtime = datetime.now()
            args = []
            nprofiles = sum([r[0] for r in results])
            lastid = max([r[1] for r in results])
            profilecount += nprofiles
            logger.log('Completed batch {0:s} at {1:f} profiles/sec.\n' \
                       .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                               nprofiles/(endtime-starttime).total_seconds()))
            logger.log('{0:d} profiles processed.\n'.format(profilecount))
            logger.log('Last profile id: {0:s}\n'.format(lastid))
    if args:
        starttime = datetime.now()
        logger.log('Starting batch at {0:s}.\n' \
                   .format(starttime.strftime('%Y-%m-%d %H:%M:%S%z')))
        results = parallelParse(args)
        endtime = datetime.now()
        args = []
        nprofiles = sum([r[0] for r in results])
        profilecount += nprofiles
        lastid = max([r[1] for r in results])
        logger.log('Completed batch {0:s} at {1:f} profiles/sec.\n' \
                   .format(endtime.strftime('%Y-%m-%d %H:%M:%S%z'),
                           nprofiles/(endtime-starttime).total_sections()))
        logger.log('{0:d} profiles processed.\n'.format(profilecount))
        logger.log('Last profile id: {0:s}\n'.format(lastid))
