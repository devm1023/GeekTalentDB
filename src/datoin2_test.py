import conf
import datoin
import sys
from logger import Logger
from datetime import datetime, timedelta
import numpy as np
from parallelize import ParallelFunction
import itertools
from pprint import pprint


def educationKey(e):
    return (e['institute'] if e['institute'] else '',
            e['degree'] if e['degree'] else '',
            e['area'] if e['area'] else '',
            e['dateFrom'] if e['dateFrom'] else 0,
            e['dateTo'] if e['dateTo'] else 0,
            e['description'] if e['description'] else '')

def experienceKey(e):
    return (e['name'] if e['name'] else '',
            e['company'] if e['company'] else '',
            e['country'] if e['country'] else '',
            e['city'] if e['city'] else '',
            e['dateFrom'] if e['dateFrom'] else 0,
            e['dateTo'] if e['dateTo'] else 0,
            e['description'] if e['description'] else '')

def makeLIProfile1(profile, dtsession, logger):
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

    # get sector
    sector = profile.get('sector', None)
    if sector is not None and type(sector) is not str:
        logger.log('invalid profile sector\n')
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

    # get crawl date
    crawledDate = profile.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False
    
    # get connections
    connections = profile.get('connections', None)
    if connections is not None and type(connections) is not str:
        logger.log('invalid profile connections\n')
        return False

    # get groups
    groups = profile.get('groups', [])
    if type(groups) is not list:
        logger.log('invalid profile groups\n')
        return False
    for group in groups:
        if type(group) is not str:
            logger.log('invalid profile groups\n')
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
        'sector'            : sector,
        'title'             : title,
        'description'       : description,
        'profileUrl'        : profileUrl,
        'profilePictureUrl' : profilePictureUrl,
        'indexedOn'         : indexedOn,
        'crawledDate'       : crawledDate,
        'connections'       : connections,
        'categories'        : categories,
        'groups'            : groups
        }
        
    # get experiences
    experiences = []
    for experience in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+profile_id+'/experiences',
            params={}, batchsize=20):
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

        # get country
        country = experience.get('country', None)
        if country is not None and type(country) is not str:
            return False

        # get city
        city = experience.get('city', None)
        if city is not None and type(city) is not str:
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
        if description is not None and type(description) is not str:
            return False

        # get timestamp
        if 'indexedOn' not in experience:
            return False
        indexedOn = experience['indexedOn']
        if type(indexedOn) is not int:
            return False

        experiences.append({
            # 'id'          : experience_id,
            # 'parentId'    : parentId,
            'name'        : name,
            'company'     : company,
            'country'     : country,
            'city'        : city,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description,
            'indexedOn'   : indexedOn})

    # get educations
    educations = []
    for education in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+profile_id+'/educations',
            params={}, batchsize=20):
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
        if 'area' in education and 'areaS' in education:
            return False
        area = education.get('area', None)
        if area is None:
            area = education.get('areaS', None)
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

        # get description
        description = education.get('description', None)
        if description is not None and type(description) is not str:
            return False

        # get timestamp
        if 'indexedOn' not in education:
            return False
        indexedOn = education['indexedOn']
        if type(indexedOn) is not int:
            return False

        educations.append({
            # 'id'          : education_id,
            # 'parentId'    : parentId,
            'institute'   : institute,
            'degree'      : degree,
            'area'        : area,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description,
            'indexedOn'   : indexedOn})

    # sort experiences and educations
    experiences.sort(key=experienceKey)
    educations.sort(key=educationKey)

    # return profile
    liprofile['experiences'] = experiences
    liprofile['educations'] = educations
    return liprofile


def makeLIProfile2(profile, logger):
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

    # get sector
    sector = profile.get('sector', None)
    if sector is not None and type(sector) is not str:
        logger.log('invalid profile sector\n')
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

    # get crawl date
    crawledDate = profile.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False
    
    # get connections
    connections = profile.get('connections', None)
    if connections is not None and type(connections) is not str:
        logger.log('invalid profile connections\n')
        return False

    # get groups
    groups = profile.get('groups', [])
    if type(groups) is not list:
        logger.log('invalid profile groups\n')
        return False
    for group in groups:
        if type(group) is not str:
            logger.log('invalid profile groups\n')
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
        'sector'            : sector,
        'title'             : title,
        'description'       : description,
        'profileUrl'        : profileUrl,
        'profilePictureUrl' : profilePictureUrl,
        'indexedOn'         : indexedOn,
        'crawledDate'       : crawledDate,
        'connections'       : connections,
        'categories'        : categories,
        'groups'            : groups,
        'experiences'       : [],
        'educations'        : []
        }

    # parse experiences and educations
    
    if 'subDocuments' not in profile:
        return liprofile

    experienceIds = set()
    educationIds = set()
    for subdocument in profile['subDocuments']:
        if 'type' not in subdocument:
            logger.log('type field missing in sub-document.\n')
            return False

        if subdocument['type'] == 'profile-experience':
            experience = subdocument
            
            # get id
            # if 'id' not in experience:
            #     logger.log('id field missing in experience.\n')
            #     return False
            # experience_id = experience['id']
            # if type(experience_id) is not str:
            #     logger.log('invalid id field in experience.\n')
            #     return False

            # get parent id
            # if 'parentId' not in experience:
            #     logger.log('parentId field missing in experience.\n')
            #     return False
            # parentId = experience['parentId']
            # if parentId != liprofile['id']:
            #     logger.log('invalid parentId field in experience.\n')
            #     return False

            # get job title
            name = experience.get('name', None)
            if name is not None and type(name) is not str:
                logger.log('invalid name field in experience.\n')
                return False

            # get company
            company = experience.get('company', None)
            if company is not None and type(company) is not str:
                logger.log('invalid company field in experience.\n')
                return False

            # get country
            country = experience.get('country', None)
            if country is not None and type(country) is not str:
                logger.log('invalid country field in experience.\n')
                return False

            # get city
            city = experience.get('city', None)
            if city is not None and type(city) is not str:
                logger.log('invalid city field in experience.\n')
                return False

            # get start date
            dateFrom = experience.get('dateFrom', None)
            if dateFrom is not None and type(dateFrom) is not int:
                logger.log('invalid dateFrom field in experience.\n')
                return False

            # get end date
            dateTo = experience.get('dateTo', None)
            if dateTo is not None and type(dateTo) is not int:
                logger.log('invalid dateTo field in experience.\n')
                return False

            # get description
            description = experience.get('description', None)
            if description is not None and type(description) is not str:
                logger.log('invalid description field in experience.\n')
                return False

            # eliminate duplicates
            # if experience_id in experienceIds:
            #     continue
            # else:
            #     experienceIds.add(experience_id)
            
            liprofile['experiences'].append({
                # 'id'          : experience_id,
                # 'parentId'    : parentId,
                'name'        : name,
                'company'     : company,
                'country'     : country,
                'city'        : city,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : liprofile['indexedOn']})
            
        elif subdocument['type'] == 'profile-education':
            education = subdocument

            # get id
            # if 'id' not in education:
            #     logger.log('id field missing in education.\n')
            #     return False
            # education_id = education['id']
            # if type(education_id) is not str:
            #     logger.log('invalid id field in education.\n')
            #     return False

            # get parent id
            # if 'parentId' not in education:
            #     logger.log('parentId field missing in education.\n')
            #     return False
            # parentId = education['parentId']
            # if parentId != liprofile['id']:
            #     logger.log('invalid parentId field in education.\n')
            #     return False

            # get institute
            institute = education.get('name', None)
            if institute is not None and type(institute) is not str:
                logger.log('invalid institute field in education.\n')
                return False

            # get degree
            degree = education.get('degree', None)
            if degree is not None and type(degree) is not str:
                logger.log('invalid degree field in education.\n')
                return False

            # get area
            area = education.get('area', None)
            if area is not None and type(area) is not str:
                logger.log('invalid area field in education.\n')
                return False

            # get start date
            dateFrom = education.get('dateFrom', None)
            if dateFrom is not None and type(dateFrom) is not int:
                logger.log('invalid dateFrom field in education.\n')
                return False

            # get end date
            dateTo = education.get('dateTo', None)
            if dateTo is not None and type(dateTo) is not int:
                logger.log('invalid dateTo field in education.\n')
                return False

            # get description
            description = education.get('description', None)
            if description is not None and type(description) is not str:
                logger.log('invalid description field in education.\n')
                return False

            # eliminate duplicates
            # if education_id in educationIds:
            #     continue
            # else:
            #     educationIds.add(education_id)

            liprofile['educations'].append({
                # 'id'          : education_id,
                # 'parentId'    : parentId,
                'institute'   : institute,
                'degree'      : degree,
                'area'        : area,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description})
            
        else:
            logger.log('unknown sub-document type.\n')
            return False

    # sort experiences and educations
    liprofile['experiences'].sort(key=experienceKey)
    liprofile['educations'].sort(key=educationKey)

    # return profile
    return liprofile

def makeINProfile1(inprofiledoc, dtsession, logger):
    # check sourceId
    if inprofiledoc.get('sourceId', '') != 'indeed':
        logger.log('invalid profile sourceId\n')
        return False

    # check type
    if inprofiledoc.get('type', '') != 'profile':
        logger.log('invalid profile type\n')
        return False
    
    # get id
    if 'id' not in inprofiledoc:
        logger.log('invalid profile id\n')
        return False
    inprofile_id = inprofiledoc['id']

    # get parentId
    if 'parentId' not in inprofiledoc:
        logger.log('invalid profile parentId\n')
        return False
    parentId = inprofiledoc['parentId']
    if parentId != inprofile_id:
        logger.log('invalid profile parentId\n')
        return False

    # get last name
    lastName = inprofiledoc.get('lastName', '')
    if type(lastName) is not str:
        logger.log('invalid profile lastName\n')
        return False

    # get first name
    firstName = inprofiledoc.get('firstName', '')
    if type(firstName) is not str:
        logger.log('invalid profile firstName\n')
        return False
    
    # get name
    name = inprofiledoc.get('name', '')
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
    country = inprofiledoc.get('country', None)
    if country is not None and type(country) is not str:
        logger.log('invalid profile country\n')
        return False

    # get city
    city = inprofiledoc.get('city', None)
    if city is not None and type(city) is not str:
        logger.log('invalid profile city\n')
        return False

    # get title
    title = inprofiledoc.get('title', None)
    if title is not None and type(title) is not str:
        logger.log('invalid profile title\n')
        return False    

    # get description
    description = inprofiledoc.get('description', None)
    if description is not None and type(description) is not str:
        logger.log('invalid profile description\n')
        return False
    
    # get inprofile url
    if 'profileUrl' not in inprofiledoc:
        logger.log('invalid profile profileUrl\n')
        return False
    profileUrl = inprofiledoc['profileUrl']
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

    # get timestamp
    if 'indexedOn' not in inprofiledoc:
        logger.log('invalid profile indexedOn\n')
        return False
    indexedOn = inprofiledoc['indexedOn']
    if type(indexedOn) is not int:
        logger.log('invalid profile indexedOn\n')
        return False

    # get crawl date
    crawledDate = inprofiledoc.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False

    inprofile = {
        'id'                : inprofile_id,
        'parentId'          : parentId,
        'lastName'          : lastName,
        'firstName'         : firstName,
        'name'              : name,
        'country'           : country,
        'city'              : city,
        'title'             : title,
        'description'       : description,
        'profileUrl'        : profileUrl,
        'indexedOn'         : indexedOn,
        'crawledDate'       : crawledDate,
        'experiences'       : [],
        'educations'        : []
        }


    # parse experiences and educations
    
    for experience in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+inprofile_id+'/experiences',
            params={}, batchsize=20):

        # get id
        if 'id' not in experience:
            logger.log('id field missing in experience.\n')
            return False
        experience_id = experience['id']
        if type(experience_id) is not str:
            logger.log('invalid id field in experience.\n')
            return False

        # get parent id
        if 'parentId' not in experience:
            logger.log('parentId field missing in experience.\n')
            return False
        parentId = experience['parentId']
        if parentId != inprofile['id']:
            logger.log('invalid parentId field in experience.\n')
            return False

        # get job title
        name = experience.get('name', None)
        if name is not None and type(name) is not str:
            logger.log('invalid name field in experience.\n')
            return False

        # get company
        company = experience.get('company', None)
        if company is not None and type(company) is not str:
            logger.log('invalid company field in experience.\n')
            return False

        # get country
        country = experience.get('country', None)
        if country is not None and type(country) is not str:
            logger.log('invalid country field in experience.\n')
            return False

        # get city
        city = experience.get('city', None)
        if city is not None and type(city) is not str:
            logger.log('invalid city field in experience.\n')
            return False

        # get start date
        dateFrom = experience.get('dateFrom', None)
        if dateFrom is not None and type(dateFrom) is not int:
            logger.log('invalid dateFrom field in experience.\n')
            return False

        # get end date
        dateTo = experience.get('dateTo', None)
        if dateTo is not None and type(dateTo) is not int:
            logger.log('invalid dateTo field in experience.\n')
            return False

        # get description
        description = experience.get('description', None)
        if description is not None and type(description) is not str:
            logger.log('invalid description field in experience.\n')
            return False

        inprofile['experiences'].append({
            # 'id'          : experience_id,
            # 'parentId'    : parentId,
            'name'        : name,
            'company'     : company,
            'country'     : country,
            'city'        : city,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description})
            
    # get educations
        
    for education in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+inprofile_id+'/educations',
            params={}, batchsize=20):

        # get id
        if 'id' not in education:
            logger.log('id field missing in education.\n')
            return False
        education_id = education['id']
        if type(education_id) is not str:
            logger.log('invalid id field in education.\n')
            return False

        # get parent id
        if 'parentId' not in education:
            logger.log('parentId field missing in education.\n')
            return False
        parentId = education['parentId']
        if parentId != inprofile['id']:
            logger.log('invalid parentId field in education.\n')
            return False

        # get institute
        institute = education.get('name', None)
        if institute is not None and type(institute) is not str:
            logger.log('invalid institute field in education.\n')
            return False

        # get degree
        degree = education.get('degree', None)
        if degree is not None and type(degree) is not str:
            logger.log('invalid degree field in education.\n')
            return False

        # get area
        area = education.get('area', None)
        if area is not None and type(area) is not str:
            logger.log('invalid area field in education.\n')
            return False

        # get start date
        dateFrom = education.get('dateFrom', None)
        if dateFrom is not None and type(dateFrom) is not int:
            logger.log('invalid dateFrom field in education.\n')
            return False

        # get end date
        dateTo = education.get('dateTo', None)
        if dateTo is not None and type(dateTo) is not int:
            logger.log('invalid dateTo field in education.\n')
            return False

        # get description
        description = education.get('description', None)
        if description is not None and type(description) is not str:
            logger.log('invalid description field in education.\n')
            return False

        inprofile['educations'].append({
            # 'id'          : education_id,
            # 'parentId'    : parentId,
            'institute'   : institute,
            'degree'      : degree,
            'area'        : area,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description})

    # sort experiences and educations
    inprofile['experiences'].sort(key=experienceKey)
    inprofile['educations'].sort(key=educationKey)

    # return inprofile
    return inprofile

def makeINProfile2(inprofiledoc, logger):
    # check sourceId
    if inprofiledoc.get('sourceId', '') != 'indeed':
        logger.log('invalid profile sourceId\n')
        return False

    # check type
    if inprofiledoc.get('type', '') != 'profile':
        logger.log('invalid profile type\n')
        return False
    
    # get id
    if 'id' not in inprofiledoc:
        logger.log('invalid profile id\n')
        return False
    inprofile_id = inprofiledoc['id']

    # get parentId
    if 'parentId' not in inprofiledoc:
        logger.log('invalid profile parentId\n')
        return False
    parentId = inprofiledoc['parentId']
    if parentId != inprofile_id:
        logger.log('invalid profile parentId\n')
        return False

    # get last name
    lastName = inprofiledoc.get('lastName', '')
    if type(lastName) is not str:
        logger.log('invalid profile lastName\n')
        return False

    # get first name
    firstName = inprofiledoc.get('firstName', '')
    if type(firstName) is not str:
        logger.log('invalid profile firstName\n')
        return False
    
    # get name
    name = inprofiledoc.get('name', '')
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
    country = inprofiledoc.get('country', None)
    if country is not None and type(country) is not str:
        logger.log('invalid profile country\n')
        return False

    # get city
    city = inprofiledoc.get('city', None)
    if city is not None and type(city) is not str:
        logger.log('invalid profile city\n')
        return False

    # get title
    title = inprofiledoc.get('title', None)
    if title is not None and type(title) is not str:
        logger.log('invalid profile title\n')
        return False    

    # get description
    description = inprofiledoc.get('description', None)
    if description is not None and type(description) is not str:
        logger.log('invalid profile description\n')
        return False
    
    # get inprofile url
    if 'profileUrl' not in inprofiledoc:
        logger.log('invalid profile profileUrl\n')
        return False
    profileUrl = inprofiledoc['profileUrl']
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

    # get timestamp
    if 'indexedOn' not in inprofiledoc:
        logger.log('invalid profile indexedOn\n')
        return False
    indexedOn = inprofiledoc['indexedOn']
    if type(indexedOn) is not int:
        logger.log('invalid profile indexedOn\n')
        return False

    # get crawl date
    crawledDate = inprofiledoc.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False

    inprofile = {
        'id'                : inprofile_id,
        'parentId'          : parentId,
        'lastName'          : lastName,
        'firstName'         : firstName,
        'name'              : name,
        'country'           : country,
        'city'              : city,
        'title'             : title,
        'description'       : description,
        'profileUrl'        : profileUrl,
        'indexedOn'         : indexedOn,
        'crawledDate'       : crawledDate,
        'experiences'       : [],
        'educations'        : []
        }


    # parse experiences and educations
    
    if 'subDocuments' not in inprofiledoc:
        return inprofile
    
    for subdocument in inprofiledoc['subDocuments']:
        if 'type' not in subdocument:
            logger.log('type field missing in sub-document.\n')
            return False

        if subdocument['type'] == 'profile-experience':
            experience = subdocument
            
            # get id
            # if 'id' not in experience:
            #     logger.log('id field missing in experience.\n')
            #     return False
            # experience_id = experience['id']
            # if type(experience_id) is not str:
            #     logger.log('invalid id field in experience.\n')
            #     return False

            # get parent id
            # if 'parentId' not in experience:
            #     logger.log('parentId field missing in experience.\n')
            #     return False
            # parentId = experience['parentId']
            # if parentId != inprofile['id']:
            #     logger.log('invalid parentId field in experience.\n')
            #     return False

            # get job title
            name = experience.get('name', None)
            if name is not None and type(name) is not str:
                logger.log('invalid name field in experience.\n')
                return False

            # get company
            company = experience.get('company', None)
            if company is not None and type(company) is not str:
                logger.log('invalid company field in experience.\n')
                return False

            # get country
            country = experience.get('country', None)
            if country is not None and type(country) is not str:
                logger.log('invalid country field in experience.\n')
                return False

            # get city
            city = experience.get('city', None)
            if city is not None and type(city) is not str:
                logger.log('invalid city field in experience.\n')
                return False

            # get start date
            dateFrom = experience.get('dateFrom', None)
            if dateFrom is not None and type(dateFrom) is not int:
                logger.log('invalid dateFrom field in experience.\n')
                return False

            # get end date
            dateTo = experience.get('dateTo', None)
            if dateTo is not None and type(dateTo) is not int:
                logger.log('invalid dateTo field in experience.\n')
                return False

            # get description
            description = experience.get('description', None)
            if description is not None and type(description) is not str:
                logger.log('invalid description field in experience.\n')
                return False

            inprofile['experiences'].append({
                # 'id'          : experience_id,
                # 'parentId'    : parentId,
                'name'        : name,
                'company'     : company,
                'country'     : country,
                'city'        : city,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description})
            
        elif subdocument['type'] == 'profile-education':
            education = subdocument

            # get id
            # if 'id' not in education:
            #     logger.log('id field missing in education.\n')
            #     return False
            # education_id = education['id']
            # if type(education_id) is not str:
            #     logger.log('invalid id field in education.\n')
            #     return False

            # get parent id
            # if 'parentId' not in education:
            #     logger.log('parentId field missing in education.\n')
            #     return False
            # parentId = education['parentId']
            # if parentId != inprofile['id']:
            #     logger.log('invalid parentId field in education.\n')
            #     return False

            # get institute
            institute = education.get('name', None)
            if institute is not None and type(institute) is not str:
                logger.log('invalid institute field in education.\n')
                return False

            # get degree
            degree = education.get('degree', None)
            if degree is not None and type(degree) is not str:
                logger.log('invalid degree field in education.\n')
                return False

            # get area
            area = education.get('area', None)
            if area is not None and type(area) is not str:
                logger.log('invalid area field in education.\n')
                return False

            # get start date
            dateFrom = education.get('dateFrom', None)
            if dateFrom is not None and type(dateFrom) is not int:
                logger.log('invalid dateFrom field in education.\n')
                return False

            # get end date
            dateTo = education.get('dateTo', None)
            if dateTo is not None and type(dateTo) is not int:
                logger.log('invalid dateTo field in education.\n')
                return False

            # get description
            description = education.get('description', None)
            if description is not None and type(description) is not str:
                logger.log('invalid description field in education.\n')
                return False

            inprofile['educations'].append({
                # 'id'          : education_id,
                # 'parentId'    : parentId,
                'institute'   : institute,
                'degree'      : degree,
                'area'        : area,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : inprofile['indexedOn']})
            
        else:
            logger.log('unknown sub-document type.\n')
            return False

    # sort experiences and educations
    inprofile['experiences'].sort(key=experienceKey)
    inprofile['educations'].sort(key=educationKey)

    # add inprofile
    return inprofile




def testRange(fromdate, todate, rows, offset, sourceId, byIndexedOn=False):
    batchsize = 10
    logger = Logger(sys.stdout)
    if sourceId is None:
        logger.log('Testing LinkedIn.\n')
        testRange(fromdate, todate, rows, offset, 'linkedin', byIndexedOn)
        logger.log('Testing Indeed.\n')
        testRange(fromdate, todate, rows, offset, 'indeed', byIndexedOn)
        return
    elif sourceId == 'linkedin':
        makeProfile1 = makeLIProfile1
        makeProfile2 = makeLIProfile2
    elif sourceId == 'indeed':
        makeProfile1 = makeINProfile1
        makeProfile2 = makeINProfile2
    else:
        raise ValueError('Invalid source id.')
    
    dtsession = datoin.Session(logger=logger)

    fromTs = int((fromdate - timestamp0).total_seconds())
    toTs   = int((todate   - timestamp0).total_seconds())
    if byIndexedOn:
        fromKey = 'fromTs'
        toKey   = 'toTs'
    else:
        fromTs *= 1000
        toTs   *= 1000
        fromKey = 'crawledFrom'
        toKey   = 'crawledTo'

    nprofiles1 = dtsession.count(url=conf.DATOIN_SEARCH,
                                 params={'sid'         : sourceId,
                                         fromKey       : fromTs,
                                         toKey         : toTs})
    nprofiles2 = dtsession.count(url=conf.DATOIN2_SEARCH,
                                 params={'sid'         : sourceId,
                                         fromKey       : fromTs,
                                         toKey         : toTs})
    logger.log(
        'Range {0:s} (ts {1:d}) to {2:s} (ts {3:d}): {4:d} profiles.\n' \
        .format(fromdate.strftime('%Y-%m-%d'), fromTs,
                todate.strftime('%Y-%m-%d'), toTs,
                nprofiles2))
    if nprofiles1 != nprofiles2:
        logger.log(
            'Number of profiles disagree: {0:d} (old API) vs {1:d} (new API)\n' \
            .format(nprofiles1, nprofiles2))
        exit(1)
    else:
        logger.log('Total counts match.\n')
    if nprofiles2 <= offset:
        return
        
    logger.log('Downloading {0:d} profiles from offset {1:d}.\n'\
               .format(rows, offset))
    profilecount = 0
    for profile2 in dtsession.query(url=conf.DATOIN2_SEARCH,
                                    params={'sid'         : sourceId,
                                            fromKey       : fromTs,
                                            toKey         : toTs},
                                    rows=rows,
                                    offset=offset):
        # construct profile from new API
        profile2 = makeProfile2(profile2, logger)
        if not profile2:
            logger.log('New API returned invalid profile at offset {0:d}\n' \
                       .format(offset+profilecount))
            exit(1)

        # construct profile from old API
        profile1 = dtsession.get(conf.DATOIN_PROFILES+'/'+profile2['id'],
                                 timeout=300).json()
        profile1 = makeProfile1(profile1, dtsession, logger)
        if not profile1:
            logger.log('Old API returned invalid profile at offset {0:d}\n' \
                       .format(offset+profilecount))
            exit(1)

        # compare profiles
        profile1 = removeIndexedOn(profile1)
        profile2 = removeIndexedOn(profile2)
        if profile1 != profile2:
            logger.log('Inconsistent data at offset {0:d}\n' \
                       .format(offset+profilecount))
            logger.log('Datoin ID: {0:s}\n'.format(profile2['id']))
            pprint(docDiff(profile1, profile2))
            exit(1)

        profilecount += 1
        if profilecount % batchsize == 0:
            logger.log('Profile {0:d} passed.\n'.format(profilecount))
    
    if profilecount % batchsize != 0:
        logger.log('Profile {0:d} passed.\n'.format(profilecount))
    

def removeIndexedOn(d):
    if isinstance(d, dict):
        newd = {}
        for key, val in d.items():
            if key != 'indexedOn':
                newd[key] = removeIndexedOn(val)
    elif isinstance(d, list):
        newd = [removeIndexedOn(v) for v in d]
    else:
        newd = d

    return newd
        
def docDiff(d1, d2):
    if not isinstance(d1, dict) or not isinstance(d2, dict):
        return (d1, d2)
    diff = {}
    keys = set(d1.keys()) | set(d2.keys())
    for key in keys:
        if key == 'indexedOn':
            continue
        val1 = d1.get(key, None)
        val2 = d2.get(key, None)
        if val1 == val2:
            continue
        if hasattr(val1, '__len__') and hasattr(val2, '__len__') \
           and len(val1) == len(val2):
            diff[key] = [docDiff(s1, s2) for s1, s2 in zip(val1, val2) \
                         if s1 != s2]
        else:
            diff[key] = (val1, val2)

    return diff

        
if __name__ == '__main__':
    # parse arguments
    try:
        sys.argv.pop(0)
        fromdate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')
        todate = datetime.strptime(sys.argv.pop(0), '%Y-%m-%d')

        sourceId = None
        byIndexedOn = False
        offset = 0
        maxoffset = None
        while sys.argv:
            option = sys.argv.pop(0).split('=')
            if len(option) == 1:
                option = option[0]
                if option == '--by-index-date':
                    byIndexedOn = True
                else:
                    raise ValueError('Invalid command line argument.')
            elif len(option) == 2:
                value=option[1]
                option=option[0]
                if option == '--source':
                    if value in ['linkedin', 'indeed']:
                        sourceId = value
                    else:
                        raise ValueError('Invalid command line argument.')
                elif option == '--offset':
                    offset = int(value)
                elif option == '--maxoffset':
                    maxoffset = int(value)
            else:
                raise ValueError('Invalid command line argument.')
    except ValueError:
        print('python3 datoin_download_profiles.py <njobs> <batchsize> '
              '<from-date> <to-date> [--by-index-date] '
              '[--source=<sourceid>] [--offset=<offset>] '
              '[--maxoffset=<maxoffset>]')
        exit(1)

    timestamp0 = datetime(year=1970, month=1, day=1)
    
    if maxoffset is None:
        rows = None
    else:
        rows = maxoffset-offset+1
    testRange(fromdate, todate, rows, offset, sourceId, byIndexedOn=byIndexedOn)
