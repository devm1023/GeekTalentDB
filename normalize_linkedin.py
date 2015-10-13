from datoindb import *
import normalformdb as nf
from windowquery import windowQuery, windows
from sqlalchemy import and_
import conf
import sys
from datetime import datetime, timedelta
from logger import Logger
from processtable import processTable

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()


def normalizeProfiles(fromid, toid, fromTs, toTs):
    batchsize = 10
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
            logger.log('Batch: {0:d} profiles processed.\n' \
                       .format(profilecount))

    # final commit
    if profilecount % batchsize != 0:
        nfdb.commit()
        logger.log('Batch: {0:d} profiles processed.\n' \
                   .format(profilecount))

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

totalrecords = dtdb.query(LIProfile.id).filter(filter).count()
logger.log('{0:d} records found.\n'.format(totalrecords))

processTable(dtdb.session, LIProfile.id, normalizeProfiles, batchsize,
             njobs=njobs, args=[fromTs, toTs],
             filter = filter, logger=logger,
             workdir='normalizejobs', prefix='linormalize')
