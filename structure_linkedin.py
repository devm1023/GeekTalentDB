import conf
from geektalentdb import *
import datoindb as dt
import sys
from datetime import datetime, timedelta
from logger import Logger
from windowquery import windowQuery, partitions
from parallelize import ParallelFunction
from logger import Logger


timestamp0 = datetime(year=1970, month=1, day=1)


# process arguments

njobs = int(sys.argv[1])
fromdate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
if len(sys.argv) > 2:
    todate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
else:
    todate = datetime.now()

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000

def structureProfiles(fromTs, toTs, id1, id2):
    windowsize = 100
    logger = Logger(sys.stdout)
    
    jobstart = datetime.now()
    logger.log(jobstart.strftime('Starting computation %Y-%m-%d %H:%M:%S%z.\n'))

    logger = Logger(sys.stdout)
    gtdb = GeekTalentDB(url=conf.GT_WRITE_DB)
    dtdb = dt.DatoinDB(url=conf.DT_READ_DB)

    q = dtdb.query(dt.LIProfile) \
            .filter(dt.LIProfile.indexedOn >= fromTs, \
                    dt.LIProfile.indexedOn < toTs)
    if id1 is not None:
        if id2 is not None:
            q = q.filter(dt.LIProfile.id >= id1, dt.LIProfile.id < id2)
        else:
            q = q.filter(dt.LIProfile.id >= id1)
        
    profilecount = 0
    for dtprofile in windowQuery(q, dt.LIProfile.id, windowsize=windowsize):
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
        if profilecount % windowsize == 0:
            gtdb.commit()
            now = datetime.now()
            deltat = (now-jobstart).total_seconds()
            logger.log('{0:d} profiles processed at {1:f} profiles/sec.\n' \
                       .format(profilecount, profilecount/deltat))
    gtdb.commit()
    now = datetime.now()
    deltat = (now-jobstart).total_seconds()
    logger.log('{0:d} profiles processed at {1:f} profiles/sec.\n' \
               .format(profilecount, profilecount/deltat))


# make batches

dtdb = dt.DatoinDB(url=conf.DT_READ_DB)
batchsize = 10
q = dtdb.query(dt.LIProfile.id) \
        .filter(dt.LIProfile.indexedOn >= fromTs, \
                dt.LIProfile.indexedOn < toTs)


if njobs > 1:
    args = [(fromTs, toTs)+p for p in partitions(q, dt.LIProfile.id, njobs)]
    ParallelFunction(structureProfiles,
                     batchsize=1,
                     workdir='sjobs',
                     prefix='listructure',
                     log=sys.stdout,
                     tries=1)(args)
else:
    structureProfiles(fromTs, toTs, None, None)

