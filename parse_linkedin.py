from datoindb import *
import normalformdb as nf
from windowquery import windowQuery
from sqlalchemy import and_
import conf
import sys
from datetime import datetime, timedelta
from logger import Logger

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()


def parseProfiles(fromTs, toTs, fromId, toId):
    batchsize = 100
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DT_READ_DB)
    nfdb = nf.NormalFormDB(url=conf.NF_WRITE_DB)

    q = dtdb.query(LIProfile)
    filter = and_(LIProfile.indexedOn >= fromTs, \
                  LIProfile.indexedOn < toTs)
    if fromid is not None:
        if toid is not None:
            filter = and_(filter,
                          LIProfile.id >= fromid,
                          LIProfile.id < toid)
        else:
            filter = and_(filter, LIProfile.id >= fromid)

    profilecount = 0
    for liprofile in windowQuery(q, LIProfile.id, filter=filter):
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
                       .format(profilecount))
            logger.log('Last profile id: {0:s}\n'.format(liprofile.id))

    # final commit
    nfdb.commit()
    logger.log('{0:d} profiles processed.\n' \
               .format(profilecount))
    logger.log('Last profile id: {0:s}\n'.format(liprofile.id))



# process arguments

fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
fromid = None
toid = None
if len(sys.argv) > 3:
    fromid = sys.argv[3]
if len(sys.argv) > 4:
    toid = sys.argv[4]

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000

parseProfiles(fromTs, toTs, fromid, toid)

