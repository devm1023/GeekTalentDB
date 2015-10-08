import conf
from geektalentdb import *
import datoindb as dt
import sys
from datetime import datetime, timedelta
from logger import Logger
from windowquery import windowQuery
from sqlalchemy import and_

timestamp0 = datetime(year=1970, month=1, day=1)


def structureProfiles(fromTs, toTs, fromid, toid):
    batchsize = 100
    logger = Logger(sys.stdout)
    
    jobstart = datetime.now()
    logger.log(jobstart.strftime('Starting computation %Y-%m-%d %H:%M:%S%z.\n'))

    logger = Logger(sys.stdout)
    gtdb = GeekTalentDB(url=conf.GT_WRITE_DB)
    dtdb = dt.DatoinDB(url=conf.DT_READ_DB)

    q = dtdb.query(dt.LIProfile)
    filter = and_(dt.LIProfile.indexedOn >= fromTs, \
                  dt.LIProfile.indexedOn < toTs)
    if fromid is not None:
        if toid is not None:
            filter = and_(filter,
                          dt.LIProfile.id >= fromid,
                          dt.LIProfile.id < toid)
        else:
            filter = and_(filter, dt.LIProfile.id >= fromid)
        
    profilecount = 0
    for dtprofile in windowQuery(q, dt.LIProfile.id, filter=filter):
        profilecount += 1

        # get location
        if dtprofile.country and dtprofile.city:
            location = ', '.join([dtprofile.city, dtprofile.country])
        elif not dtprofile.country and not dtprofile.city:
            location = None
        elif not dtprofile.country:
            location = dtprofile.city
        else:
            location = dtprofile.country

        # get experiences
        experiences = []
        for dtexperience in dtdb.query(dt.Experience) \
                                .filter(dt.Experience.parentId == dtprofile.id):
            # get start date
            if not dtexperience.dateFrom:
                startdate = None
            else:
                startdate \
                    = timestamp0 + timedelta(milliseconds=dtexperience.dateFrom)

            # get end date
            if not dtexperience.dateTo:
                enddate = None
            else:
                enddate \
                    = timestamp0 + timedelta(milliseconds=dtexperience.dateTo)

            experiences.append({'company'     : dtexperience.company,
                                'title'       : dtexperience.name,
                                'description' : dtexperience.description,
                                'startdate'   : startdate,
                                'enddate'     : enddate})

        # remove truncated skills
        skills = [s for s in dtprofile.categories \
                  if len(s) < 4 or s[-3:] != '...']

        # add profile
        gtdb.addLIProfile(dtprofile.parentId,
                          dtprofile.name,
                          dtprofile.title,
                          dtprofile.description,
                          location,
                          dtprofile.profileUrl,
                          dtprofile.profilePictureUrl,
                          skills,
                          experiences)

        # commit
        if profilecount % batchsize == 0:
            gtdb.commit()
            now = datetime.now()
            deltat = (now-jobstart).total_seconds()
            logger.log('{0:d} profiles processed at {1:f} profiles/sec.\n' \
                       .format(profilecount, profilecount/deltat))
            logger.log('Last profile: {0:s}\n'.format(dtprofile.id))
    gtdb.commit()
    now = datetime.now()
    deltat = (now-jobstart).total_seconds()
    logger.log('{0:d} profiles processed at {1:f} profiles/sec.\n' \
               .format(profilecount, profilecount/deltat))
    logger.log('Last profile: {0:s}\n'.format(dtprofile.id))


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

    
structureProfiles(fromTs, toTs, fromid, toid)

