import conf
from geektalentdb import *
import datoindb as dt
import sys
from datetime import datetime, timedelta
from logger import Logger
from windowquery import windowQuery


timestamp0 = datetime(year=1970, month=1, day=1)
logger = Logger(sys.stdout)

windowsize = 100
maxprofiles = 500


# connect to databases

gtdb = GeekTalentDB(url=conf.GT_WRITE_DB)
dtdb = dt.DatoinDB(url=conf.DT_READ_DB)


# compute timestamps

fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
if len(sys.argv) > 2:
    todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
else:
    todate = datetime.now()

fromTs = int((fromdate - timestamp0).total_seconds())*1000
toTs   = int((todate   - timestamp0).total_seconds())*1000


profilecount = 0
for dtprofile in windowQuery(
        dtdb.query(dt.LIProfile) \
            .filter(dt.LIProfile.indexedOn >= fromTs,
                    dt.LIProfile.indexedOn < toTs),
        dt.LIProfile.id,
        windowsize):
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

    
    # add profile
    gtdb.addLIProfile(dtprofile.parentId,
                      dtprofile.name,
                      dtprofile.title,
                      dtprofile.description,
                      location,
                      dtprofile.profileUrl,
                      dtprofile.profilePictureUrl,
                      dtprofile.categories,
                      experiences)

    # commit
    logger.log('{0:d} profiles processed.\n'.format(profilecount))
    if profilecount % windowsize == 0:
        gtdb.commit()

    if profilecount >= maxprofiles:
        break
gtdb.commit()

