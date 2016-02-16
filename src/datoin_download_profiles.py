import conf
from datoindb import *
import datoin
import sys
from logger import Logger
from datetime import datetime, timedelta
import numpy as np
from parallelize import ParallelFunction
import itertools
import argparse


def addLIProfile(dtdb, liprofiledoc, dtsession, logger):
    # check sourceId
    if liprofiledoc.get('sourceId', '') != 'linkedin':
        logger.log('invalid profile sourceId\n')
        return False

    # check type
    if liprofiledoc.get('type', '') != 'profile':
        logger.log('invalid profile type\n')
        return False
    
    # get id
    if 'id' not in liprofiledoc:
        logger.log('invalid profile id\n')
        return False
    liprofile_id = liprofiledoc['id']

    # get parentId
    if 'parentId' not in liprofiledoc:
        logger.log('invalid profile parentId\n')
        return False
    parentId = liprofiledoc['parentId']
    if parentId != liprofile_id:
        logger.log('invalid profile parentId\n')
        return False

    # get last name
    lastName = liprofiledoc.get('lastName', '')
    if type(lastName) is not str:
        logger.log('invalid profile lastName\n')
        return False

    # get first name
    firstName = liprofiledoc.get('firstName', '')
    if type(firstName) is not str:
        logger.log('invalid profile firstName\n')
        return False
    
    # get name
    name = liprofiledoc.get('name', '')
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
    country = liprofiledoc.get('country', None)
    if country is not None and type(country) is not str:
        logger.log('invalid profile country\n')
        return False

    # get city
    city = liprofiledoc.get('city', None)
    if city is not None and type(city) is not str:
        logger.log('invalid profile city\n')
        return False

    # get sector
    sector = liprofiledoc.get('sector', None)
    if sector is not None and type(sector) is not str:
        logger.log('invalid profile sector\n')
        return False
    
    # get title
    title = liprofiledoc.get('title', None)
    if title is not None and type(title) is not str:
        logger.log('invalid profile title\n')
        return False    

    # get description
    description = liprofiledoc.get('description', None)
    if description is not None and type(description) is not str:
        logger.log('invalid profile description\n')
        return False
    
    # get liprofile url
    if 'profileUrl' not in liprofiledoc:
        logger.log('invalid profile profileUrl\n')
        return False
    profileUrl = liprofiledoc['profileUrl']
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

    # get liprofiledoc picture url
    profilePictureUrl = liprofiledoc.get('profilePictureUrl', None)
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
    if 'indexedOn' not in liprofiledoc:
        logger.log('invalid profile indexedOn\n')
        return False
    indexedOn = liprofiledoc['indexedOn']
    if type(indexedOn) is not int:
        logger.log('invalid profile indexedOn\n')
        return False

    # get crawl date
    crawledDate = liprofiledoc.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False
    
    # get connections
    connections = liprofiledoc.get('connections', None)
    if connections is not None and type(connections) is not str:
        logger.log('invalid profile connections\n')
        return False

    # get groups
    groups = liprofiledoc.get('groups', [])
    if type(groups) is not list:
        logger.log('invalid profile groups\n')
        return False
    for group in groups:
        if type(group) is not str:
            logger.log('invalid profile groups\n')
            return False

    # get skills
    categories = liprofiledoc.get('categories', [])
    if type(categories) is not list:
        logger.log('invalid profile categories\n')
        return False
    for skill in categories:
        if type(skill) is not str:
            logger.log('invalid profile categories\n')
            return False

    liprofile = {
        'id'                : liprofile_id,
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
    
    if 'subDocuments' not in liprofiledoc:
        dtdb.addLIProfile(liprofile)
        return True
    
    for subdocument in liprofiledoc['subDocuments']:
        if 'type' not in subdocument:
            logger.log('type field missing in sub-document.\n')
            return False

        if subdocument['type'] == 'profile-experience':
            experience = subdocument
            
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
            if parentId != liprofile['id']:
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

            liprofile['experiences'].append({
                'id'          : experience_id,
                'parentId'    : parentId,
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
            if parentId != liprofile['id']:
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

            liprofile['educations'].append({
                'id'          : education_id,
                'parentId'    : parentId,
                'institute'   : institute,
                'degree'      : degree,
                'area'        : area,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : liprofile['indexedOn']})
            
        else:
            logger.log('unknown sub-document type.\n')
            return False

    # add liprofile
    dtdb.addLIProfile(liprofile)
    return True


def addINProfile(dtdb, inprofiledoc, dtsession, logger):
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
        dtdb.addINProfile(inprofile)
        return True
    
    for subdocument in inprofiledoc['subDocuments']:
        if 'type' not in subdocument:
            logger.log('type field missing in sub-document.\n')
            return False

        if subdocument['type'] == 'profile-experience':
            experience = subdocument
            
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
                'id'          : experience_id,
                'parentId'    : parentId,
                'name'        : name,
                'company'     : company,
                'country'     : country,
                'city'        : city,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : inprofile['indexedOn']})
            
        elif subdocument['type'] == 'profile-education':
            education = subdocument

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
                'id'          : education_id,
                'parentId'    : parentId,
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

    # add inprofile
    dtdb.addINProfile(inprofile)
    return True

def addUWProfile(dtdb, uwprofiledoc, dtsession, logger):
    # check sourceId
    if uwprofiledoc.get('sourceId', '') != 'indeed':
        logger.log('invalid profile sourceId\n')
        return False

    # check type
    if uwprofiledoc.get('type', '') != 'profile':
        logger.log('invalid profile type\n')
        return False
    
    # get id
    if 'id' not in uwprofiledoc:
        logger.log('invalid profile id\n')
        return False
    uwprofile_id = uwprofiledoc['id']

    # get parentId
    if 'parentId' not in uwprofiledoc:
        logger.log('invalid profile parentId\n')
        return False
    parentId = uwprofiledoc['parentId']
    if parentId != uwprofile_id:
        logger.log('invalid profile parentId\n')
        return False

    # get last name
    lastName = uwprofiledoc.get('lastName', '')
    if type(lastName) is not str:
        logger.log('invalid profile lastName\n')
        return False

    # get first name
    firstName = uwprofiledoc.get('firstName', '')
    if type(firstName) is not str:
        logger.log('invalid profile firstName\n')
        return False
    
    # get name
    name = uwprofiledoc.get('name', '')
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
    country = uwprofiledoc.get('country', None)
    if country is not None and type(country) is not str:
        logger.log('invalid profile country\n')
        return False

    # get city
    city = uwprofiledoc.get('city', None)
    if city is not None and type(city) is not str:
        logger.log('invalid profile city\n')
        return False

    # get title
    title = uwprofiledoc.get('title', None)
    if title is not None and type(title) is not str:
        logger.log('invalid profile title\n')
        return False    

    # get description
    description = uwprofiledoc.get('description', None)
    if description is not None and type(description) is not str:
        logger.log('invalid profile description\n')
        return False
    
    # get uwprofile url
    if 'profileUrl' not in uwprofiledoc:
        logger.log('invalid profile profileUrl\n')
        return False
    profileUrl = uwprofiledoc['profileUrl']
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

    # get uwprofiledoc picture url
    profilePictureUrl = uwprofiledoc.get('profilePictureUrl', None)
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
    if 'indexedOn' not in uwprofiledoc:
        logger.log('invalid profile indexedOn\n')
        return False
    indexedOn = uwprofiledoc['indexedOn']
    if type(indexedOn) is not int:
        logger.log('invalid profile indexedOn\n')
        return False

    # get crawl date
    crawledDate = uwprofiledoc.get('crawledDate', None)
    if crawledDate is not None and type(crawledDate) is not int:
        logger.log('invalid profile crawledDate\n')
        return False

    uwprofile = {
        'id'                : uwprofile_id,
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
        'crawledDate'       : crawledDate,
        'experiences'       : [],
        'educations'        : [],
        'tests'             : []
        }


    # parse experiences and educations
    
    if 'subDocuments' not in uwprofiledoc:
        dtdb.addINProfile(uwprofile)
        return True
    
    for subdocument in uwprofiledoc['subDocuments']:
        if 'type' not in subdocument:
            logger.log('type field missing in sub-document.\n')
            return False

        if subdocument['type'] == 'profile-experience':
            experience = subdocument
            
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
            if parentId != uwprofile['id']:
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

            uwprofile['experiences'].append({
                'id'          : experience_id,
                'parentId'    : parentId,
                'name'        : name,
                'company'     : company,
                'country'     : country,
                'city'        : city,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : uwprofile['indexedOn']})
            
        elif subdocument['type'] == 'profile-education':
            education = subdocument

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
            if parentId != uwprofile['id']:
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

            uwprofile['educations'].append({
                'id'          : education_id,
                'parentId'    : parentId,
                'institute'   : institute,
                'degree'      : degree,
                'area'        : area,
                'dateFrom'    : dateFrom,
                'dateTo'      : dateTo,
                'description' : description,
                'indexedOn'   : uwprofile['indexedOn']})

        elif subdocument['type'] == 'profile-test':
            # get id
            if 'id' not in test:
                logger.log('id field missing in test.\n')
                return False
            test_id = test['id']
            if type(test_id) is not str:
                logger.log('invalid id field in test.\n')
                return False

            # get parent id
            if 'parentId' not in test:
                logger.log('parentId field missing in test.\n')
                return False
            parentId = test['parentId']
            if parentId != uwprofile['id']:
                logger.log('invalid parentId field in test.\n')
                return False

            # get name
            name = test.get('name', None)
            if name is not None and type(name) is not str:
                logger.log('invalid name field in test.\n')
                return False

            # get type
            testtype = test.get('type', None)
            if testtype is not None and type(testtype) is not str:
                logger.log('invalid type field in test.\n')
                return False

            # get type
            score = test.get('score', None)
            if score is not None and type(score) is not float:
                logger.log('invalid score field in test.\n')
                return False

            # get timestamp
            if 'indexedOn' not in test:
                return False
            indexedOn = test['indexedOn']
            if type(indexedOn) is not int:
                return False

            uwprofile['tests'].append({
                'id'          : test_id,
                'parentId'    : parentId,
                'name'        : name,
                'type'        : testtype,
                'score'       : score,
                'indexedOn'   : indexedOn})
            
        else:
            logger.log('unknown sub-document type.\n')
            return False

    # add uwprofile
    dtdb.addINProfile(uwprofile)
    return True


def downloadProfiles(fromTs, toTs, offset, rows, byIndexedOn, sourceId):
    if conf.MAX_PROFILES is not None:
        rows = min(rows, conf.MAX_PROFILES)
    
    logger = Logger(sys.stdout)
    BATCH_SIZE = 100
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    dtsession = datoin.Session(logger=logger)

    if byIndexedOn:
        fromKey = 'fromTs'
        toKey   = 'toTs'
    else:
        fromKey = 'crawledFrom'
        toKey   = 'crawledTo'
    params = {fromKey : fromTs, toKey : toTs, 'sid' : sourceId}
    if sourceId == 'linkedin':
        addProfile = addLIProfile
    elif sourceId == 'indeed':
        addProfile = addINProfile
    elif sourceId == 'upwork':
        addProfile = addUWProfile
    else:
        raise ValueError('Invalid source id.')
    
    logger.log('Downloading {0:d} profiles from offset {1:d}.\n'\
               .format(rows, offset))
    failed_offsets = []
    count = 0
    for liprofiledoc in dtsession.query(url=conf.DATOIN2_SEARCH,
                                        params=params,
                                        rows=rows,
                                        offset=offset):
        if not addProfile(dtdb, liprofiledoc, dtsession, logger):
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
        logger.log('Re-processing {0:d} profiles.\n' \
                   .format(len(failed_offsets)))
        new_failed_offsets = []
        count = 0
        for offset in failed_offsets:
            count += 1
            try:
                liprofiledoc \
                    = next(dtsession.query(url=conf.DATOIN2_SEARCH,
                                           params=params,
                                           rows=1,
                                           offset=offset))
            except StopIteration:
                new_failed_offsets.append(offset)
                continue
            if not addLIProfile(dtdb, liprofiledoc, dtsession, logger):
                new_failed_offsets.append(offset)

            if count % BATCH_SIZE == 0:
                logger.log('{0:d} profiles processed.\n'.format(count))
                dtdb.commit()
        dtdb.commit()

        failed_offsets = new_failed_offsets

    logger.log('failed offsets: {0:s}\n'.format(str(failed_offsets)))
    return failed_offsets


def downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, sourceId,
                  offset=0, maxoffset=None):
    logger = Logger(sys.stdout)
    if sourceId is None:
        logger.log('Downloading LinkedIn profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'linkedin',
                      offset=offset, maxoffset=maxoffset)
        logger.log('Downloading Indeed profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'indeed',
                      offset=offset, maxoffset=maxoffset)
        logger.log('Downloading Upwork profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'upwork',
                      offset=offset, maxoffset=maxoffset)
        return
    
    fromTs = int((tfrom - timestamp0).total_seconds())
    toTs   = int((tto   - timestamp0).total_seconds())
    if byIndexedOn:
        fromKey = 'fromTs'
        toKey   = 'toTs'
    else:
        fromTs *= 1000
        toTs   *= 1000
        fromKey = 'crawledFrom'
        toKey   = 'crawledTo'
    params = {fromKey : fromTs, toKey : toTs, 'sid' : sourceId}
    
    nprofiles = datoin.count(url=conf.DATOIN2_SEARCH, params=params)
    logger.log(
        'Range {0:s} (ts {1:d}) to {2:s} (ts {3:d}): {4:d} profiles.\n' \
        .format(tfrom.strftime('%Y-%m-%d'), fromTs,
                tto.strftime('%Y-%m-%d'), toTs,
                nprofiles))
    if nprofiles <= offset:
        return
    if maxoffset is not None:
        nprofiles = min(nprofiles, maxoffset)

    offsets = list(range(offset, nprofiles, maxprofiles))
    offsets.append(nprofiles)
    for offset1, offset2 in zip(offsets[:-1], offsets[1:]):
        dlstart = datetime.now()    
        logger.log('Starting download for offsets {0:d} to {1:d} at {2:s}.\n' \
                   .format(offset1, offset2-1,
                           dlstart.strftime('%Y-%m-%d %H:%M:%S%z')))

        ncurrentjobs = min(njobs, offset2-offset1)
        if ncurrentjobs > 1:
            poffsets = np.linspace(offset1, offset2, ncurrentjobs+1, dtype=int)
            args = [(fromTs, toTs, a, b-a, byIndexedOn, sourceId) \
                    for a, b in zip(poffsets[:-1], poffsets[1:])]
            results = ParallelFunction(downloadProfiles,
                                       batchsize=1,
                                       workdir='jobs',
                                       prefix='lidownload',
                                       tries=1)(args)
            failedoffsets = list(itertools.chain(*results))
        else:
            failedoffsets = downloadProfiles(fromTs, toTs, offset1,
                                             offset2-offset1,
                                             byIndexedOn, sourceId)

        dlend = datetime.now()
        dltime = (dlend-dlstart).total_seconds()
        logger.log(dlend.strftime('Finished download %Y-%m-%d %H:%M:%S%z'))
        if dltime > 0:
            logger.log(' at {0:f} profiles/sec.\n' \
                       .format((offset2-offset1)/dltime))
        else:
            logger.log('.\n')

        if failedoffsets:
            logger.log('Failed offsets: {0:s}.\n'.format(repr(failedoffsets)))


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('njobs', help='Number of parallel jobs.', type=int)
    parser.add_argument('batchsize', help='Number of rows per batch.', type=int)
    parser.add_argument('--from-date', help=
                        'Only process profiles crawled or indexed on or after\n'
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only process profiles crawled or indexed before\n'
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--by-index-date', help=
                        'Indicates that the dates specified with --fromdate and\n'
                        '--todate are index dates. Otherwise they are interpreted\n'
                        'as crawl dates.',
                        action='store_true')
    parser.add_argument('--from-offset', type=int, default=0, help=
                        'Start processing from this offset. Useful for\n'
                        'crash recovery.')
    parser.add_argument('--to-offset', help=
                        'Stop processing at this offset.')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork'],
                        help=
                        'Source type to process. If not specified all sources are\n'
                        'processed.')
    args = parser.parse_args()

    njobs = max(args.njobs, 1)
    batchsize = args.batchsize
    try:
        fromdate = datetime.strptime(args.from_date, '%Y-%m-%d')
        if not args.to_date:
            todate = datetime.now()
        else:
            todate = datetime.strptime(args.to_date, '%Y-%m-%d')
    except ValueError:
        sys.stderr.write('Invalid date format.\n')
        exit(1)
    byIndexedOn = bool(args.by_index_date)
    offset = args.from_offset
    maxoffset = args.to_offset
    sourceId = args.source    
        
    timestamp0 = datetime(year=1970, month=1, day=1)
        
    if offset == 0 and maxoffset is None:
        deltat = timedelta(days=1)
        t = fromdate
        while t < todate:
            downloadRange(t, min(t+deltat, todate), njobs, njobs*batchsize,
                          byIndexedOn, sourceId)
            t += deltat
    else:
        downloadRange(fromdate, todate, njobs, njobs*batchsize,
                      byIndexedOn, sourceId,
                      offset=offset, maxoffset=maxoffset)
