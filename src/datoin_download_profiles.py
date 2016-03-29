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

class FieldError(Exception):
    pass

def getField(d, name, fieldtype, required=False, default=None,
             docname='profile'):
    logger = Logger(sys.stdout)
    isPresent = name in d
    if required and not isPresent:
        msg = 'Missing field `{0:s}` in {1:s} documnent.' \
              .format(name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)
    isList = False
    if isinstance(fieldtype, list):
        fieldtype = fieldtype[0]
        isList = True
    isUrl = False
    if fieldtype == 'url':
        fieldtype = str
        isUrl = True

    result = d.get(name, default)

    # workaround for API bug
    if isList and isinstance(result, dict) and len(result) == 1 \
       and 'myArrayList' in result:
        result = result['myArrayList']
        
    if isPresent and isList and not isinstance(result, list):
        msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} documnent.' \
              .format(repr(result), name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)

    if isPresent:
        if isList:
            vals = result
        else:
            vals = [result]
    else:
        vals = []
    for r in vals:
        if fieldtype is float and isinstance(r, int):
            r = float(r)
        if not isinstance(r, fieldtype):
            msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} documnent.' \
                  .format(repr(r), name, docname)
            logger.log(msg+'\n')
            raise FieldError(msg)
        if isUrl:
            if len(r) < 4 or r[:4] != 'http':
                msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} '
                'documnent.'.format(repr(result), name, docname)
                logger.log(msg+'\n')
                raise FieldError(msg)

    return result

def checkField(d, name, value=None, docname='profile'):
    logger = Logger(sys.stdout)
    if name not in d:
        msg = 'Missing field `{0:s}` in {1:s} documnent.' \
              .format(name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)
    if value is not None and not isinstance(value, list):
        value = [value]
    if value is not None and d[name] not in value:
        msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} documnent.' \
              .format(repr(d[name]), name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)

def nonEmpty(d):
    return not all(v is None for v in d.values())

    
def addLIProfile(dtdb, liprofiledoc, dtsession, logger):
    try:
        checkField(liprofiledoc, 'sourceId', 'linkedin')
        checkField(liprofiledoc, 'type', 'profile')
        liprofile = {}
        for name, fieldtype, required in \
            [('profileId',   str,   True),
             ('crawlNumber', int,   True),
             ('name',        str,   False),
             ('firstName',   str,   False),
             ('lastName',    str,   False),
             ('country',     str,   False),
             ('city',        str,   False),
             ('sector',      str,   False),
             ('title',       str,   False),
             ('description', str,   False),
             ('profileUrl',  'url', False),
             ('profilePictureUrl', 'url', False),
             ('connections', str,   False),
             ('categories',  [str], False),
             ('indexedOn',   int,   True),
             ('crawledDate', int,   False),
             ('crawlFailCount', int, True),
            ]:
            liprofile[name] = getField(liprofiledoc, name, fieldtype,
                                       required=required)

        liprofile['experiences'] = []
        liprofile['educations']  = []
        liprofile['groups']      = []
        for subdocument in liprofiledoc.get('subDocuments', []):
            checkField(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-group'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('url',         'url', False),
                     ('company',     str,   False),
                     ('country',     str,   False),
                     ('city',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    experience[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='experience')
                if nonEmpty(experience):
                    liprofile['experiences'].append(experience)
                    
            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('url',         'url', False),
                     ('degree',      str,   False),
                     ('area',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    education[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='education')
                if nonEmpty(education):
                    liprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-group':
                group = {}
                for name, fieldtype, required in \
                    [('name',      str,   True),
                     ('url',       'url', False),
                    ]:
                    group[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='group')
                if nonEmpty(group):
                    liprofile['groups'].append(group)
                        
    except FieldError:
        return False

    # add liprofile
    dtdb.addFromDict(liprofile, LIProfile)
    return True


def addINProfile(dtdb, inprofiledoc, dtsession, logger):
    try:
        checkField(inprofiledoc, 'sourceId', 'indeed')
        checkField(inprofiledoc, 'type', 'profile')
        inprofile = {}
        for name, fieldtype, required in \
            [('profileId',   str,   True),
             ('crawlNumber', int,   True),
             ('name',        str,   False),
             ('firstName',   str,   False),
             ('lastName',    str,   False),
             ('country',     str,   False),
             ('city',        str,   False),
             ('title',       str,   False),
             ('description', str,   False),
             ('additionalInformation', str, False),
             ('profileUrl',  'url', False),
             ('profileUpdatedDate', int, False),
             ('indexedOn',   int,   True),
             ('crawledDate', int,   True),
             ('crawlFailCount', int, True),
            ]:
            inprofile[name] = getField(inprofiledoc, name, fieldtype,
                                       required=required)

        inprofile['experiences']    = []
        inprofile['educations']     = []
        inprofile['certifications'] = []
        for subdocument in inprofiledoc.get('subDocuments', []):
            checkField(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-certification'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('company',     str,   False),
                     ('country',     str,   False),
                     ('city',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    experience[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='experience')
                if nonEmpty(experience):
                    inprofile['experiences'].append(experience)
                    
            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('degree',      str,   False),
                     ('area',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    education[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='education')
                if nonEmpty(education):
                    inprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-certification':
                certification = {}
                for name, fieldtype, required in \
                    [('name',        str,   True),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    certification[name] = getField(subdocument, name, fieldtype,
                                                   required=required,
                                                   docname='certification')
                if nonEmpty(certification):
                    inprofile['certifications'].append(certification)
                        
    except FieldError:
        return False

    # add inprofile
    dtdb.addFromDict(inprofile, INProfile)
    return True


def addUWProfile(dtdb, uwprofiledoc, dtsession, logger):
    try:
        checkField(uwprofiledoc, 'sourceId', 'upwork')
        checkField(uwprofiledoc, 'type', 'profile')
        uwprofile = {}
        for name, fieldtype, required in \
            [('profileId',   str,   True),
             ('crawlNumber', int,   True),
             ('name',        str,   False),
             ('firstName',   str,   False),
             ('lastName',    str,   False),
             ('country',     str,   False),
             ('city',        str,   False),
             ('title',       str,   False),
             ('description', str,   False),
             ('profileUrl',  'url', False),
             ('profilePictureUrl', 'url', False),
             ('categories',  [str], False),
             ('indexedOn',   int,   True),
             ('crawledDate', int,   True),
             ('crawlFailCount', int, True),
            ]:
            uwprofile[name] = getField(uwprofiledoc, name, fieldtype,
                                       required=required)

        uwprofile['experiences']    = []
        uwprofile['educations']     = []
        uwprofile['tests']          = []
        for subdocument in uwprofiledoc.get('subDocuments', []):
            checkField(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-tests'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('company',     str,   False),
                     ('country',     str,   False),
                     ('city',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    experience[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='experience')
                if nonEmpty(experience):
                    uwprofile['experiences'].append(experience)
                    
            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('degree',      str,   False),
                     ('area',        str,   False),
                     ('dateFrom',    int,   False),
                     ('dateTo',      int,   False),
                     ('description', str,   False),
                    ]:
                    education[name] = getField(subdocument, name, fieldtype,
                                               required=required,
                                               docname='education')
                if nonEmpty(education):
                    uwprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-tests':
                test = {}
                for name, fieldtype, required in \
                    [('name',        str,   False),
                     ('score',       float, False),
                    ]:
                    test[name] = getField(subdocument, name, fieldtype,
                                          required=required,
                                          docname='test')
                if nonEmpty(test):
                    uwprofile['tests'].append(test)
                        
    except FieldError:
        return False

    # add uwprofile
    dtdb.addFromDict(uwprofile, UWProfile)
    return True

def addMUProfile(dtdb, muprofiledoc, dtsession, logger):
    try:
        checkField(muprofiledoc, 'sourceId', 'meetup')
        checkField(muprofiledoc, 'type', 'profile')
        muprofile = {}
        for name, fieldtype, required in \
            [('profileId',   str,   True),
             ('crawlNumber', int,   True),
             ('name',        str,   False),
             ('country',     str,   False),
             ('city',        str,   False),
             ('latitude',    float, False),
             ('longitude',   float, False),
             ('status',      str,   False),
             ('description', str,   False),
             ('profileUrl',  'url', False),
             ('profilePictureId', str, False),
             ('profilePictureUrl', 'url', False),
             ('profileHQPictureUrl', 'url', False),
             ('profileThumbPictureUrl', 'url', False),
             ('categories',  [str], False),
             ('indexedOn',   int,   True),
             ('crawledDate', int,   True),
             ('crawlFailCount', int, True),
            ]:
            muprofile[name] = getField(muprofiledoc, name, fieldtype,
                                       required=required)

        muprofile['groups']     = []
        muprofile['events']     = []
        muprofile['comments']   = []
        muprofile['links']      = []        
        for subdocument in muprofiledoc.get('subDocuments', []):
            checkField(subdocument, 'type',
                       ['member-group', 'group', 'event', 'comment'])
            if subdocument['type'] in ['member-group', 'group']:
                group = {}
                for name, fieldtype, required in \
                    [('country',     str,   False),
                     ('city',        str,   False),
                     ('latitude',    float, False),
                     ('longitude',   float, False),
                     ('timezone',    str,   False),
                     ('utcOffset',   int,   False),
                     ('name',        str,   False),
                     ('categoryName', str, False),
                     ('categoryShortname', str, False),
                     ('categoryId',  str,   False),
                     ('description', str,   False),
                     ('url',         'url', False),
                     ('urlname',     str,   False),
                     ('pictureUrl',  'url', False),
                     ('pictureId',   int,   False),
                     ('HQPictureUrl', 'url', False),
                     ('thumbPictureUrl', 'url', False),
                     ('joinMode',    str,   False),
                     ('rating',      float, False),
                     ('organizerName', str, False),
                     ('organizerId', str,   False),
                     ('members',     int,   False),
                     ('state',       str,   False),
                     ('visibility',  str,   False),
                     ('who',         str,   False),
                     ('categories',  [str], False),
                     ('createdDate', int,   False),
                    ]:
                    group[name] = getField(subdocument, name, fieldtype,
                                           required=required,
                                           docname='group')
                if nonEmpty(group):
                    muprofile['groups'].append(group)

            elif subdocument['type'] == 'event':
                event = {}
                for name, fieldtype, required in \
                    [('country',      str,   False),
                     ('city',         str,   False),
                     ('addressLine1', str,   False),
                     ('addressLine2', str,   False),
                     ('latitude',     float, False),
                     ('longitude',    float, False),
                     ('phone',        str,   False),
                     ('name',         str,   False),
                     ('description',  str,   False),
                     ('url',          'url', False),
                     ('time',         int,   False),
                     ('utcOffset',    int,   False),
                     ('status',       str,   False),
                     ('headcount',    int,   False),
                     ('visibility',   str,   False),
                     ('rsvpLimit',    int,   False),
                     ('yesRsvpCount', int,   False),
                     ('maybeRsvpCount', int, False),
                     ('waitlistCount', int,  False),
                     ('ratingCount',  int,   False),
                     ('ratingCount',  int,   False),
                     ('ratingAverage', float, False),
                     ('feeRequired',  str,   False),
                     ('feeCurrency',  str,   False),
                     ('feeLabel',     str,   False),
                     ('feeDescription', str, False),
                     ('feeAccepts',   str,   False),
                     ('feeAmount',    float, False),
                     ('createdDate',  int,   False),
                    ]:
                    event[name] = getField(subdocument, name, fieldtype,
                                           required=required,
                                           docname='event')
                if nonEmpty(event):
                    muprofile['events'].append(event)

            elif subdocument['type'] == 'comment':
                comment = {}
                for name, fieldtype, required in \
                    [('createdDate', int,   False),
                     ('inReplyTo',   str,   False),
                     ('description', str,   False),
                     ('url',         'url', False),
                    ]:
                    comment[name] = getField(subdocument, name, fieldtype,
                                          required=required,
                                          docname='comment')
                if nonEmpty(comment):
                    muprofile['comments'].append(comment)

        for subdocument in muprofiledoc.get('otherProfiles', []):
            link = {}
            for name, fieldtype, required in \
                    [('type',        str,   True),
                     ('url',         str,   True),
                    ]:
                link[name] = getField(subdocument, name, fieldtype,
                                      required=required,
                                      docname='link')
            if nonEmpty(link):
                muprofile['links'].append(link)

    except FieldError:
        return False

    # add muprofile
    muprofile = dtdb.addFromDict(muprofile, MUProfile)
    dtdb.commit()
    return True


def addGHProfile(dtdb, ghprofiledoc, dtsession, logger):
    try:
        checkField(ghprofiledoc, 'sourceId', 'github')
        checkField(ghprofiledoc, 'type', 'profile')
        ghprofile = {}
        for name, fieldtype, required in \
            [('profileId',   str,   True),
             ('crawlNumber', int,   True),
             ('name',        str,   False),
             ('country',     str,   False),
             ('city',        str,   False),
             ('company',     str,   False),
             ('createdDate', int,   False),
             ('profileUrl',  'url', False),
             ('profilePictureUrl', 'url', False),
             ('login',       str,   False),
             ('email',       str,   False),
             ('contributionsCount', int, False),
             ('followersCount', int, False),
             ('followingCount', int, False),
             ('publicRepoCount', int, False),
             ('publicGistCount', int, False),
             ('indexedOn',   int,   True),
             ('crawledDate', int,   True),
            ]:
            ghprofile[name] = getField(ghprofiledoc, name, fieldtype,
                                       required=required)

        ghprofile['repositories']     = []
        ghprofile['links']            = []
        for subdocument in ghprofiledoc.get('subDocuments', []):
            checkField(subdocument, 'type', ['repository'])
            if subdocument['type'] == 'repository':
                repository = {}
                for name, fieldtype, required in \
                    [('name',         str,   False),
                     ('description',  str,   False),
                     ('fullName',     str,   False),
                     ('url',          'url', False),
                     ('gitUrl',       str,   False),
                     ('sshUrl',       str,   False),
                     ('createdDate',  int,   False),
                     ('pushedDate',   int,   False),
                     ('size',         int,   False),
                     ('defaultBranch', str,  False),
                     ('viewCount',    int,  False),
                     ('subscribersCount', int,  False),
                     ('forksCount',   int,  False),
                     ('stargazersCount', int, False),
                     ('openIssuesCount', int, False),
                     ('tags',         [str], False),
                    ]:
                    repository[name] = getField(subdocument, name, fieldtype,
                                                required=required,
                                                docname='repository')
                if nonEmpty(repository):
                    ghprofile['repositories'].append(repository)

        for subdocument in ghprofiledoc.get('otherProfiles', []):
            link = {}
            for name, fieldtype, required in \
                    [('type',        str,   True),
                     ('url',         str,   True),
                    ]:
                link[name] = getField(subdocument, name, fieldtype,
                                      required=required,
                                      docname='link')
            if nonEmpty(link):
                ghprofile['links'].append(link)

    except FieldError:
        return False

    # add ghprofile
    dtdb.addFromDict(ghprofile, GHProfile)
    return True


def downloadProfiles(fromTs, toTs, maxprofiles, byIndexedOn, sourceId):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    dtsession = datoin.Session(logger=logger)
    BATCH_SIZE = 100

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
    elif sourceId == 'meetup':
        addProfile = addMUProfile
    elif sourceId == 'github':
        addProfile = addGHProfile
    else:
        raise ValueError('Invalid source id.')
    
    logger.log('Downloading profiles from timestamp {0:d} to {1:d}.\n'\
               .format(fromTs, toTs))
    count = 0
    failedProfiles = []
    for profiledoc in dtsession.query(url=conf.DATOIN3_SEARCH,
                                      params=params):
        if 'profileId' not in profiledoc:
            raise IOError('Encountered profile without profileId.')
        profileId = profiledoc['profileId']
        if 'crawlNumber' not in profiledoc:
            raise IOError('Encountered profile without crawlNumber.')
        crawlNumber = profiledoc['crawlNumber']
        
        if not addProfile(dtdb, profiledoc, dtsession, logger):
            logger.log('Failed profile {0:s}|{1:s}.\n' \
                       .format(str(profileId), str(crawlNumber)))
            failedProfiles.append((profileId, crawlNumber))
        count += 1

        # commit
        if count % BATCH_SIZE == 0:
            logger.log('{0:d} profiles processed.\n'.format(count))
            dtdb.commit()
        if maxprofiles is not None and count >= maxprofiles:
            break
    if count % BATCH_SIZE != 0:
        logger.log('{0:d} profiles processed.\n'.format(count))
    dtdb.commit()

    return count, failedProfiles


def downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, sourceId):
    logger = Logger(sys.stdout)
    njobs = max(njobs, 1)
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
        logger.log('Downloading Meetup profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'meetup',
                      offset=offset, maxoffset=maxoffset)
        logger.log('Downloading GitHub profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'github',
                      offset=offset, maxoffset=maxoffset)
        return
    
    fromTs = int((tfrom - timestamp0).total_seconds())
    toTs   = int((tto   - timestamp0).total_seconds())
    if not byIndexedOn:
        fromTs *= 1000
        toTs *= 1000
    timestamps = np.linspace(fromTs, toTs, njobs+1, dtype=int)

    dlstart = datetime.now()
    logger.log('Downloading time range {0:s} to {1:s}.\n' \
               .format(tfrom.strftime('%Y-%m-%d'),
                       tto.strftime('%Y-%m-%d')))
    logger.log('Starting at {0:s}.\n' \
               .format(dlstart.strftime('%Y-%m-%d %H:%M:%S%z')))

    if njobs > 1:
        args = [(ts1, ts2, maxprofiles, byIndexedOn, sourceId) \
                for ts1, ts2 in zip(timestamps[:-1], timestamps[1:])]
        results = ParallelFunction(downloadProfiles,
                                   batchsize=1,
                                   workdir='jobs',
                                   prefix='lidownload',
                                   tries=1)(args)
        count = 0
        failedProfiles = []
        for c, fp in results:
            count += c
            failedProfiles.extend(fp)
    else:
        count, failedProfiles = downloadProfiles(fromTs, toTs, maxprofiles,
                                                 byIndexedOn, sourceId)

    dlend = datetime.now()
    dltime = (dlend-dlstart).total_seconds()
    logger.log(dlend.strftime('Finished download %Y-%m-%d %H:%M:%S%z'))
    if dltime > 0:
        logger.log(' at {0:f} profiles/sec.\n' \
                   .format(count/dltime))
    else:
        logger.log('.\n')

    if failedProfiles:
        logger.log('failed profiles:\n')
        for profileId, crawlNumber in failedProfiles:
            logger.log('{0:s}|{0:s}'.format(str(profileId), str(crawlNumber)))

    return count


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', default=1, type=int,
                        help='Number of parallel jobs.')
    parser.add_argument('--step-size', default=1, type=int,
                        help='Time increment in days.')
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
    parser.add_argument('--max-profiles', type=int,
                        help='Maximum number of profiles to download')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork', 'meetup',
                                 'github'],
                        help=
                        'Source type to process. If not specified all sources are\n'
                        'processed.')
    args = parser.parse_args()

    njobs = max(args.jobs, 1)
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
    maxprofiles = args.max_profiles
    sourceId = args.source
    stepSize = args.step_size
        
    timestamp0 = datetime(year=1970, month=1, day=1)

    deltat = timedelta(days=stepSize)
    profilecount = 0
    t = fromdate
    if maxprofiles is None:
        while t < todate:
            profilecount \
                += downloadRange(t, min(t+deltat, todate), njobs,
                                 None, byIndexedOn, sourceId)
            t += deltat
    else:
        while t < todate and profilecount < maxprofiles:
            profilecount \
                += downloadRange(t, min(t+deltat, todate), njobs,
                                 maxprofiles-profilecount, byIndexedOn,
                                 sourceId)
            t += deltat

