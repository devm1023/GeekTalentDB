import conf
from datoindb import *
import datoin
import sys
from logger import Logger
from datetime import datetime, timedelta
import numpy as np
from parallelize import ParallelFunction
import dill
dill.settings['recurse'] = True


# calculate timestamps
timestamp0 = datetime(year=1970, month=1, day=1)
njobs = int(sys.argv[1])
fromdate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
if len(sys.argv) > 3:
    todate = datetime.strptime(sys.argv[3], '%Y-%m-%d')
else:
    todate = datetime.now()
fromTs = int((fromdate - timestamp0).total_seconds())
toTs   = int((todate   - timestamp0).total_seconds())


def addProfile(dtdb, profile, dtsession, logger):
    # check sourceId
    if profile.get('sourceId', '') != 'linkedin':
        logger.log('invalid profile sourceId\n')
        return False

    # check type
    if profile.get('type', '') != 'profile':
        logger.log('invalid profile type\n')
        return False
    
    # get id
    if 'id' not in profile:
        logger.log('invalid profile id\n')
        return False
    profile_id = profile['id']

    # get parentId
    if 'parentId' not in profile:
        logger.log('invalid profile parentId\n')
        return False
    parentId = profile['parentId']
    if parentId != profile_id:
        logger.log('invalid profile parentId\n')
        return False

    # get last name
    lastName = profile.get('lastName', '')
    if type(lastName) is not str:
        logger.log('invalid profile lastName\n')
        return False

    # get first name
    firstName = profile.get('firstName', '')
    if type(firstName) is not str:
        logger.log('invalid profile firstName\n')
        return False
    
    # get name
    name = profile.get('name', '')
    if not name:
        name = ' '.join([firstName, lastName])
    if name == ' ':
        logger.log('invalid profile name\n')
        return False
    if not firstName:
        firstName = None
    if not lastName:
        lastName = None

    # get country
    country = profile.get('country', None)
    if country is not None and type(country) is not str:
        logger.log('invalid profile country\n')
        return False

    # get city
    city = profile.get('city', None)
    if city is not None and type(city) is not str:
        logger.log('invalid profile city\n')
        return False

    # get title
    title = profile.get('title', None)
    if title is not None and type(title) is not str:
        logger.log('invalid profile title\n')
        return False    

    # get description
    description = profile.get('description', None)
    if description is not None and type(description) is not str:
        logger.log('invalid profile description\n')
        return False
    
    # get profile url
    if 'profileUrl' not in profile:
        logger.log('invalid profile profileUrl\n')
        return False
    profileUrl = profile['profileUrl']
    if profileUrl is not None and type(profileUrl) is not str:
        logger.log('invalid profile profileUrl\n')
        return False
    try:
        if profileUrl[:4].lower() != 'http':
            logger.log('invalid profile profileUrl\n')
            return False
    except IndexError:
        logger.log('invalid profile profileUrl\n')
        return False

    # get profile picture url
    profilePictureUrl = profile.get('profilePictureUrl', None)
    if profilePictureUrl is not None and type(profilePictureUrl) is not str:
        logger.log('invalid profile profilePictureUrl\n')
        return False
    try:
        if profilePictureUrl is not None and \
           profilePictureUrl[:4].lower() != 'http':
            logger.log('invalid profile profilePictureUrl\n')
            return False
    except IndexError:
        logger.log('invalid profile profilePictureUrl\n')
        return False

    # get timestamp
    if 'indexedOn' not in profile:
        logger.log('invalid profile indexedOn\n')
        return False
    indexedOn = profile['indexedOn']
    if type(indexedOn) is not int:
        logger.log('invalid profile indexedOn\n')
        return False

    # get connections
    connections = profile.get('connections', None)
    if connections is not None and type(connections) is not str:
        logger.log('invalid profile connections\n')
        return False    

    # get skills
    categories = profile.get('categories', [])
    if type(categories) is not list:
        logger.log('invalid profile categories\n')
        return False
    for skill in categories:
        if type(skill) is not str:
            logger.log('invalid profile categories\n')
            return False

    liprofile = {
        'id'                : profile_id,
        'parentId'          : parentId,
        'lastName'          : lastName,
        'firstName'         : firstName,
        'name'              : name,
        'country'           : country,
        'city'              : city,
        'title'             : title,
        'description'       : description,
        'profileUrl'        : profileUrl,
        'profilePictureUrl' : profilePictureUrl,
        'indexedOn'         : indexedOn,
        'connections'       : connections,
        'categories'        : categories,
        }
        
    # get experiences
    experiences = []
    for experience in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+profile_id+'/experiences',
            params={}):
        # get id
        if 'id' not in experience:
            return False
        experience_id = experience['id']
        if type(experience_id) is not str:
            return False

        # get parent id
        if 'parentId' not in experience:
            return False
        parentId = experience['parentId']
        if parentId != liprofile['id']:
            return False
        
        # get job title
        name = experience.get('name', None)
        if name is not None and type(name) is not str:
            return False

        # get company
        company = experience.get('company', None)
        if company is not None and type(company) is not str:
            return False

        # get start date
        dateFrom = experience.get('dateFrom', None)
        if dateFrom is not None and type(dateFrom) is not int:
            return False

        # get end date
        dateTo = experience.get('dateTo', None)
        if dateTo is not None and type(dateTo) is not int:
            return False

        # get description
        description = experience.get('description', None)
        if type(description) is not str:
            continue

        # get timestamp
        if 'indexedOn' not in experience:
            return False
        indexedOn = experience['indexedOn']
        if type(indexedOn) is not int:
            return False

        experiences.append({
            'id'          : experience_id,
            'parentId'    : parentId,
            'name'        : name,
            'company'     : company,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description,
            'indexedOn'   : indexedOn})

    # get educations
    educations = []
    for education in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+profile_id+'/educations',
            params={}):
        # get id
        if 'id' not in education:
            return False
        education_id = education['id']
        if type(education_id) is not str:
            return False

        # get parent id
        if 'parentId' not in education:
            return False
        parentId = education['parentId']
        if parentId != liprofile['id']:
            return False
        
        # get institute
        institute = education.get('name', None)
        if institute is not None and type(institute) is not str:
            return False

        # get degree
        degree = education.get('degree', None)
        if degree is not None and type(degree) is not str:
            return False

        # get area
        area = education.get('area', None)
        if area is not None and type(area) is not str:
            return False
        
        # get start date
        dateFrom = education.get('dateFrom', None)
        if dateFrom is not None and type(dateFrom) is not int:
            return False

        # get end date
        dateTo = education.get('dateTo', None)
        if dateTo is not None and type(dateTo) is not int:
            return False

        # get timestamp
        if 'indexedOn' not in education:
            return False
        indexedOn = education['indexedOn']
        if type(indexedOn) is not int:
            return False

        educations.append({
            'id'          : education_id,
            'parentId'    : parentId,
            'institute'   : institute,
            'degree'      : degree,
            'area'        : area,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'indexedOn'   : indexedOn})


    # add profile
    dtdb.addLIProfile(liprofile, experiences, educations)
    return True


def downloadLinkedin(fromTs, toTs, offset, rows):
    if conf.MAX_PROFILES is not None:
        rows = min(rows, conf.MAX_PROFILES)
    
    logger = Logger(sys.stdout)
    BATCH_SIZE = 10
    dtdb = DatoinDB(url=conf.DT_WRITE_DB)
    dtsession = datoin.Session()

    logger.log('Downloading {0:d} profiles.\n'.format(rows))
    failed_offsets = []
    count = 0
    for profile in dtsession.query(params={'sid'    : 'linkedin',
                                           'fromTs' : fromTs,
                                           'toTs'   : toTs},
                                   rows=rows,
                                   offset=offset):
        if not addProfile(dtdb, profile, dtsession, logger):
            logger.log('Failed at offset {0:d}.\n'.format(offset+count))
            failed_offsets.append(offset+count)
        count += 1

        # commit
        if count % BATCH_SIZE == 0:
            logger.log('{0:d} profiles processed.\n'.format(count))
            dtdb.commit()
    dtdb.commit()

    for attempt in range(conf.MAX_ATTEMPTS):
        if not failed_offsets:
            break
        logger.log('Re-processing {0:d} profiles.\n'.format(len(failed_offsets)))
        new_failed_offsets = []
        count = 0
        for offset in failed_offsets:
            count += 1
            try:
                profile = next(dtsession.query(params={'sid'    : 'linkedin',
                                                       'fromTs' : fromTs,
                                                       'toTs'   : toTs},
                                               rows=1,
                                               offset=offset))
            except StopIteration:
                new_failed_offsets.append(offset)
                continue
            if not addProfile(dtdb, profile, dtsession, logger):
                new_failed_offsets.append(offset)

            if count % BATCH_SIZE == 0:
                logger.log('{0:d} profiles processed.\n'.format(count))
                dtdb.commit()
        dtdb.commit()

        failed_offsets = new_failed_offsets

    logger.log('failed offsets: {0:s}\n'.format(str(failed_offsets)))
    return failed_offsets


nprofiles = datoin.count(params={'sid'    : 'linkedin',
                                 'fromTs' : fromTs,
                                 'toTs'   : toTs})
sys.stdout.write('{0:d} profiles found.\n'.format(nprofiles))
sys.stdout.flush()


if njobs > 1:
    offsets = np.linspace(0, nprofiles, njobs+1, dtype=int)
    args = [(fromTs, toTs, os1, os2-os1) \
            for os1, os2 in zip(offsets[:-1], offsets[1:])]
    results = ParallelFunction(downloadLinkedin,
                               njobs=njobs,
                               workdir='jobs',
                               prefix='lidownload',
                               tries=1,
                               autocancel=False,
                               log=sys.stdout,
                               cleanup=1)(args)
    sys.stdout.write('Failed offsets:\n')
    for i, r in enumerate(results):
        sys.stdout.write('job {0:03d}: {1:s}\n'.format(i, str(r)))
    sys.stdout.flush()
else:
    downloadLinkedin(fromTs, toTs, 0, nprofiles)
