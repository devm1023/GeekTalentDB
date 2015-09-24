import conf
from geektalentdb import *
import datoin
import sys
from datetime import datetime, timedelta
from logger import Logger


timestamp0 = datetime(year=1970, month=1, day=1)
logger = Logger(sys.stdout)


# connect to database
        
gtdb = GeekTalentDB(url=conf.WRITE_DB_URL)

fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
if len(sys.argv) > 2:
    todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
else:
    todate = datetime.now()

fromTs = int((fromdate - timestamp0).total_seconds())
toTs   = int((todate   - timestamp0).total_seconds())


profilecount = 0
for profile in datoin.query(params={'sid' : 'linkedin',
                                    'fromTs' : fromTs,
                                    'toTs'   : toTs},
                            rows=conf.MAX_PROFILES):
    profilecount += 1
    # get id
    if 'parentId' not in profile:
        continue
    parent_id = profile['parentId']
    
    # get profile url
    if 'profileUrl' not in profile:
        continue
    try:
        if profile['profileUrl'][:4].lower() != 'http':
            continue
    except IndexError:
        continue
    url = profile['profileUrl']

    # get name
    name = profile.get('name', '')
    if not name:
        name = ' '.join([profile.get('firstName', ''),
                         profile.get('lastName', '')])
    if name == ' ':
        continue

    # get skills
    skills = profile.get('categories', [])
    if type(skills) is not list:
        continue

    # get experiences
    experiences = []
    for experience in datoin.query(
            url=conf.DATOIN_PROFILES+'/'+profile['parentId']+'/experiences',
            params={}):
        # get company
        company = experience.get('company', None)
        if company is not None and type(company) is not str:
            continue
        
        # get job title
        title = experience.get('name', None)
        if type(title) is not str or not title:
            continue

        # get description
        description = experience.get('description', None)
        if type(description) is not str:
            continue

        # get start date
        startdate = experience.get('dateFrom', None)
        if startdate is not None and type(startdate) is not int:
            continue
        if startdate == 0:
            startdate = None
        if startdate is not None:
            startdate = timestamp0 + timedelta(milliseconds=startdate)

        # get end date
        enddate = experience.get('dateTo', None)
        if enddate is not None and type(enddate) is not int:
            continue
        if enddate == 0:
            enddate = None
        if enddate is not None:
            enddate = timestamp0 + timedelta(milliseconds=enddate)

        experiences.append({'company'     : company,
                            'title'       : title,
                            'description' : description,
                            'startdate'   : startdate,
                            'enddate'     : enddate})

    
    # add profile
    gtdb.add_liprofile(parent_id, name, None, None, url,
                       skills, experiences)

    # commit
    if profilecount % 100 == 0:
        gtdb.commit()
        logger.log('{0:d} profiles processed.\n'.format(profilecount))


