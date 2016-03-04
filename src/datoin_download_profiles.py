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
            [('id',          str,   True),
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
            [('id',          str,   True),
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
            [('id',          str,   True),
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
                        'profile-test'])
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

            elif subdocument['type'] == 'profile-test':
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
            [('id',          str,   True),
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
                     ('categoryId',  int,   False),
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
            [('id',          str,   True),
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
    elif sourceId == 'meetup':
        addProfile = addMUProfile
    elif sourceId == 'github':
        addProfile = addGHProfile
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
            if not addProfile(dtdb, liprofiledoc, dtsession, logger):
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
        logger.log('Downloading Meetup profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'meetup',
                      offset=offset, maxoffset=maxoffset)
        logger.log('Downloading GitHub profiles.\n')
        downloadRange(tfrom, tto, njobs, maxprofiles, byIndexedOn, 'github',
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
    parser.add_argument('--to-offset', type=int, help=
                        'Stop processing at this offset.')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork', 'meetup',
                                 'github'],
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
