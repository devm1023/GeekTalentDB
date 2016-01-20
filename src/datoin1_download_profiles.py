import conf
from datoindb import *
import datoin
import sys
from logger import Logger
from datetime import datetime, timedelta
import numpy as np
from parallelize import ParallelFunction
import itertools


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


    # parse experiences
    
    for experience in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+liprofile_id+'/experiences',
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

    # get educations
        
    for education in dtsession.query(
            url=conf.DATOIN_PROFILES+'/'+liprofile_id+'/educations',
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
            'id'          : education_id,
            'parentId'    : parentId,
            'institute'   : institute,
            'degree'      : degree,
            'area'        : area,
            'dateFrom'    : dateFrom,
            'dateTo'      : dateTo,
            'description' : description,
            'indexedOn'   : inprofile['indexedOn']})

    # add inprofile
    dtdb.addINProfile(inprofile)
    return True


def downloadProfiles(fromTs, toTs, offset, rows, byIndexedOn, sourceId):
    if conf.MAX_PROFILES is not None:
        rows = min(rows, conf.MAX_PROFILES)
    
    logger = Logger(sys.stdout)
    BATCH_SIZE = 10
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
    else:
        raise ValueError('Invalid source id.')
    
    logger.log('Downloading {0:d} profiles from offset {1:d}.\n'\
               .format(rows, offset))
    failed_offsets = []
    count = 0
    for liprofiledoc in dtsession.query(url=conf.DATOIN_SEARCH,
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
                    = next(dtsession.query(url=DATOIN2_SEARCH,
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
    try:
        sys.argv.pop(0)
        njobs = max(int(sys.argv.pop(0)), 1)
        batchsize = int(sys.argv.pop(0))
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
