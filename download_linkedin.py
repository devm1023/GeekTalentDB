import conf
from datoindb import *
import datoin
import sys
from logger import Logger
from datetime import datetime, timedelta

logger = Logger(sys.stdout)

# connect to database
dtdb = DatoinDB(url=conf.DT_WRITE_DB)

# calculate timestamps
timestamp0 = datetime(year=1970, month=1, day=1)
fromdate = datetime.strptime(sys.argv[1], '%Y-%m-%d')
if len(sys.argv) > 2:
    todate = datetime.strptime(sys.argv[2], '%Y-%m-%d')
else:
    todate = datetime.now()
fromTs = int((fromdate - timestamp0).total_seconds())
toTs   = int((todate   - timestamp0).total_seconds())


def add_profile(dtdb, profile):
    # get id
    if 'id' not in profile:
        logger.log('invalid profile id\n')
        return False
    profile_id = profile['id']

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
        'profileUrl'        : profileUrl,
        'profilePictureUrl' : profilePictureUrl,
        'indexedOn'         : indexedOn,
        'connections'       : connections,
        'categories'        : categories,
        }
        
    # get experiences
    experiences = []
    for experience in datoin.query(
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
    for education in datoin.query(
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
    dtdb.add_liprofile(liprofile, experiences, educations)
    return True

    
current_offset = 0
failed_offsets = []
for profile in datoin.query(params={'sid' : 'linkedin',
                                    'fromTs' : fromTs,
                                    'toTs'   : toTs},
                            rows=conf.MAX_PROFILES):
    if not add_profile(dtdb, profile):
        failed_offsets.append(current_offset)
    current_offset += 1

    # commit
    logger.log('{0:d} profiles processed.\n'.format(current_offset))
    if current_offset % 100 == 0:
        dtdb.commit()

logger.log('failed offsets: {0:s}\n'.format(str(failed_offsets)))
