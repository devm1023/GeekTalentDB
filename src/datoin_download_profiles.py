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

def get_field(d, name, fieldtype, required=False, default=None,
             docname='profile'):
    logger = Logger(sys.stdout)
    is_present = name in d
    if required and not is_present:
        msg = 'Missing field `{0:s}` in {1:s} documnent.' \
              .format(name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)
    is_list = False
    if isinstance(fieldtype, list):
        fieldtype = fieldtype[0]
        is_list = True
    is_url = False
    if fieldtype == 'url':
        fieldtype = str
        is_url = True

    result = d.get(name, default)

    # workaround for API bug
    if is_list and isinstance(result, dict) and len(result) == 1 \
       and 'my_array_list' in result:
        result = result['my_array_list']

    if is_present and is_list and not isinstance(result, list):
        msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} documnent.' \
              .format(repr(result), name, docname)
        logger.log(msg+'\n')
        raise FieldError(msg)

    if is_present:
        if is_list:
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
        if is_url:
            if len(r) < 4 or r[:4] != 'http':
                msg = 'Invalid value {0:s} for field `{1:s}` in {2:s} '
                'documnent.'.format(repr(result), name, docname)
                logger.log(msg+'\n')
                raise FieldError(msg)

    return result

def check_field(d, name, value=None, docname='profile'):
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

def non_empty(d):
    return not all(v is None for v in d.values())


def add_liprofile(dtdb, liprofiledoc, dtsession, logger):
    try:
        check_field(liprofiledoc, 'sourceId', 'linkedin')
        check_field(liprofiledoc, 'type', 'profile')
        liprofile = {}
        for name, fieldname, fieldtype, required in \
            [('profile_id',   'profileId',   str,   True),
             ('crawl_number', 'crawlNumber', int,   True),
             ('name',         'name',        str,   False),
             ('first_name',   'firstName',   str,   False),
             ('last_name',    'lastName',    str,   False),
             ('country',      'country',     str,   False),
             ('city',         'city',        str,   False),
             ('sector',       'sector',      str,   False),
             ('title',        'title',       str,   False),
             ('description',  'description', str,   False),
             ('profile_url',  'profileUrl',  'url', False),
             ('profile_picture_url', 'profilePictureUrl', 'url', False),
             ('connections',  'connections', str,   False),
             ('categories',   'categories',  [str], False),
             ('indexed_on',   'indexedOn',   int,   True),
             ('crawled_date', 'crawledDate', int,   False),
             ('crawl_fail_count', 'crawlFailCount', int, True),
            ]:
            liprofile[name] = get_field(liprofiledoc, fieldname, fieldtype,
                                        required=required)

        liprofile['experiences'] = []
        liprofile['educations']  = []
        liprofile['groups']      = []
        for subdocument in liprofiledoc.get('sub_documents', []):
            check_field(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-group'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('url',         'url',         'url', False),
                     ('company',     'company',     str,   False),
                     ('country',     'country',     str,   False),
                     ('city',        'city',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    experience[name] = get_field(subdocument, fieldname,
                                                 fieldtype,
                                                 required=required,
                                                 docname='experience')
                if non_empty(experience):
                    liprofile['experiences'].append(experience)

            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('url',         'url',         'url', False),
                     ('degree',      'degree',      str,   False),
                     ('area',        'area',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    education[name] = get_field(subdocument, fieldname,
                                                fieldtype,
                                                required=required,
                                                docname='education')
                if non_empty(education):
                    liprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-group':
                group = {}
                for name, fieldtype, required in \
                    [('name',      'name',      str,   True),
                     ('url',       'url',       'url', False),
                    ]:
                    group[name] = get_field(subdocument, fieldname, fieldtype,
                                            required=required,
                                            docname='group')
                if non_empty(group):
                    liprofile['groups'].append(group)

    except FieldError:
        return False

    # add liprofile
    dtdb.add_from_dict(liprofile, LIProfile)
    return True


def add_inprofile(dtdb, inprofiledoc, dtsession, logger):
    try:
        check_field(inprofiledoc, 'sourceId', 'indeed')
        check_field(inprofiledoc, 'type', 'profile')
        inprofile = {}
        for name, fieldname, fieldtype, required in \
            [('profile_id',   'profileId',   str,   True),
             ('crawl_number', 'crawlNumber', int,   True),
             ('name',         'name',        str,   False),
             ('first_name',   'firstName',   str,   False),
             ('last_name',    'lastName',    str,   False),
             ('country',      'country',     str,   False),
             ('city',         'city',        str,   False),
             ('title',        'title',       str,   False),
             ('description',  'description', str,   False),
             ('additional_information', 'additionalInformation', str, False),
             ('profile_url',  'profileUrl',  'url', False),
             ('profile_updated_date', 'profileUpdatedDate', int, False),
             ('indexed_on',   'indexedOn',   int,   True),
             ('crawled_date', 'crawledDate', int,   True),
             ('crawl_fail_count', 'crawlFailCount', int, True),
            ]:
            inprofile[name] = get_field(inprofiledoc, fieldname, fieldtype,
                                        required=required)

        inprofile['experiences']    = []
        inprofile['educations']     = []
        inprofile['certifications'] = []
        for subdocument in inprofiledoc.get('sub_documents', []):
            check_field(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-certification'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('company',     'company',     str,   False),
                     ('country',     'country',     str,   False),
                     ('city',        'city',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    experience[name] = get_field(subdocument, fieldname,
                                                 fieldtype,
                                                 required=required,
                                                 docname='experience')
                if non_empty(experience):
                    inprofile['experiences'].append(experience)

            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('degree',      'degree',      str,   False),
                     ('area',        'area',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    education[name] = get_field(subdocument, fieldname,
                                                fieldtype,
                                                required=required,
                                                docname='education')
                if non_empty(education):
                    inprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-certification':
                certification = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   True),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    certification[name] = get_field(subdocument, fieldname,
                                                    fieldtype,
                                                    required=required,
                                                    docname='certification')
                if non_empty(certification):
                    inprofile['certifications'].append(certification)

    except FieldError:
        return False

    # add inprofile
    dtdb.add_from_dict(inprofile, INProfile)
    return True


def add_uwprofile(dtdb, uwprofiledoc, dtsession, logger):
    try:
        check_field(uwprofiledoc, 'sourceId', 'upwork')
        check_field(uwprofiledoc, 'type', 'profile')
        uwprofile = {}
        for name, fieldname, fieldtype, required in \
            [('profile_id',   'profileId',   str,   True),
             ('crawl_number', 'crawlNumber', int,   True),
             ('name',         'name',        str,   False),
             ('first_name',   'firstName',   str,   False),
             ('last_name',    'lastName',    str,   False),
             ('country',      'country',     str,   False),
             ('city',         'city',        str,   False),
             ('title',        'title',       str,   False),
             ('description',  'description', str,   False),
             ('profile_url',  'profileUrl',  'url', False),
             ('profile_picture_url', 'profilePictureUrl', 'url', False),
             ('categories',   'categories',  [str], False),
             ('indexed_on',   'indexedOn',   int,   True),
             ('crawled_date', 'crawledDate', int,   True),
             ('crawl_fail_count', 'crawlFailCount', int, True),
            ]:
            uwprofile[name] = get_field(uwprofiledoc, fieldname, fieldtype,
                                       required=required)

        uwprofile['experiences']    = []
        uwprofile['educations']     = []
        uwprofile['tests']          = []
        for subdocument in uwprofiledoc.get('sub_documents', []):
            check_field(subdocument, 'type',
                       ['profile-experience',
                        'profile-education',
                        'profile-tests'])
            if subdocument['type'] == 'profile-experience':
                experience = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('company',     'company',     str,   False),
                     ('country',     'country',     str,   False),
                     ('city',        'city',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    experience[name] = get_field(subdocument, fieldname,
                                                 fieldtype,
                                                 required=required,
                                                 docname='experience')
                if non_empty(experience):
                    uwprofile['experiences'].append(experience)

            elif subdocument['type'] == 'profile-education':
                education = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('degree',      'degree',      str,   False),
                     ('area',        'area',        str,   False),
                     ('date_from',   'dateFrom',    int,   False),
                     ('date_to',     'dateTo',      int,   False),
                     ('description', 'description', str,   False),
                    ]:
                    education[name] = get_field(subdocument, fieldname,
                                                fieldtype,
                                                required=required,
                                                docname='education')
                if non_empty(education):
                    uwprofile['educations'].append(education)

            elif subdocument['type'] == 'profile-tests':
                test = {}
                for name, fieldname, fieldtype, required in \
                    [('name',        'name',        str,   False),
                     ('score',       'score',       float, False),
                     ('test_percentile', 'testPercentile', float, False),
                     ('test_date',   'testDate',    int,   False),
                     ('test_duration', 'testDuration', float, False)
                    ]:
                    test[name] = get_field(subdocument, fieldname, fieldtype,
                                           required=required,
                                           docname='test')
                if non_empty(test):
                    uwprofile['tests'].append(test)

    except FieldError:
        return False

    # add uwprofile
    dtdb.add_from_dict(uwprofile, UWProfile)
    return True

def add_muprofile(dtdb, muprofiledoc, dtsession, logger):
    try:
        check_field(muprofiledoc, 'sourceId', 'meetup')
        check_field(muprofiledoc, 'type', 'profile')
        muprofile = {}
        for name, fieldname, fieldtype, required in \
            [('profile_id',  'profileId',   str,   True),
             ('crawl_number', 'crawlNumber', int,   True),
             ('name',        'name',        str,   False),
             ('country',     'country',     str,   False),
             ('city',        'city',        str,   False),
             ('latitude',    'latitude',    float, False),
             ('longitude',   'longitude',   float, False),
             ('status',      'status',      str,   False),
             ('description', 'description', str,   False),
             ('profile_url', 'profileUrl',  'url', False),
             ('profile_picture_id', 'profilePictureId', str, False),
             ('profile_picture_url', 'profilePictureUrl', 'url', False),
             ('profile_hqpicture_url', 'profileHQPictureUrl', 'url', False),
             ('profile_thumb_picture_url', 'profileThumbPictureUrl', 'url',
              False),
             ('categories',  'categories',  [str], False),
             ('indexed_on',  'indexedOn',   int,   True),
             ('crawled_date', 'crawledDate', int,   True),
             ('crawl_fail_count', 'crawlFailCount', int, True),
            ]:
            muprofile[name] = get_field(muprofiledoc, fieldname, fieldtype,
                                        required=required)

        muprofile['groups']     = []
        muprofile['events']     = []
        muprofile['comments']   = []
        muprofile['links']      = []
        for subdocument in muprofiledoc.get('sub_documents', []):
            check_field(subdocument, 'type',
                       ['member-group', 'group', 'event', 'comment'])
            if subdocument['type'] in ['member-group', 'group']:
                group = {}
                for name, fieldname, fieldtype, required in \
                    [('country',     'country',     str,   False),
                     ('city',        'city',        str,   False),
                     ('latitude',    'latitude',    float, False),
                     ('longitude',   'longitude',   float, False),
                     ('timezone',    'timezone',    str,   False),
                     ('utc_offset',  'utcOffset',   int,   False),
                     ('name',        'name',        str,   False),
                     ('category_name', 'categoryName', str, False),
                     ('category_shortname', 'categoryShortname', str, False),
                     ('category_id', 'categoryId',  str,   False),
                     ('description', 'description', str,   False),
                     ('url',         'url',         'url', False),
                     ('urlname',     'urlname',     str,   False),
                     ('picture_url', 'pictureUrl',  'url', False),
                     ('picture_id',  'pictureId',   int,   False),
                     ('hqpicture_url', 'HQPictureUrl', 'url', False),
                     ('thumb_picture_url', 'url', False),
                     ('join_mode',   'joinMode',    str,   False),
                     ('rating',      'rating',      float, False),
                     ('organizer_name', 'organizerName', str, False),
                     ('organizer_id', 'organizerId', str,   False),
                     ('members',     'members',     int,   False),
                     ('state',       'state',       str,   False),
                     ('visibility',  'visibility',  str,   False),
                     ('who',         'who',         str,   False),
                     ('categories',  'categories',  [str], False),
                     ('created_date', 'createdDate', int,   False),
                    ]:
                    group[name] = get_field(subdocument, fieldname, fieldtype,
                                           required=required,
                                           docname='group')
                if non_empty(group):
                    muprofile['groups'].append(group)

            elif subdocument['type'] == 'event':
                event = {}
                for name, fieldname, fieldtype, required in \
                    [('country',      'country',      str,   False),
                     ('city',         'city',         str,   False),
                     ('address_line1', 'addressLine1', str,   False),
                     ('address_line2', 'addressLine2', str,   False),
                     ('latitude',     'latitude',     float, False),
                     ('longitude',    'longitude',    float, False),
                     ('phone',        'phone',        str,   False),
                     ('name',         'name',         str,   False),
                     ('description',  'description',  str,   False),
                     ('url',          'url',          'url', False),
                     ('time',         'time',         int,   False),
                     ('utc_offset',   'utcOffset',    int,   False),
                     ('status',       'status',       str,   False),
                     ('headcount',    'headcount',    int,   False),
                     ('visibility',   'visibility',   str,   False),
                     ('rsvp_limit',   'rsvpLimit',    int,   False),
                     ('yes_rsvp_count', 'yesRsvpCount', int,   False),
                     ('maybe_rsvp_count', 'maybeRsvpCount', int, False),
                     ('waitlist_count', 'waitlistCount', int,  False),
                     ('rating_count',  'ratingCount',  int,   False),
                     ('rating_average', 'ratingAverage', float, False),
                     ('fee_required', 'feeRequired',  str,   False),
                     ('fee_currency', 'feeCurrency',  str,   False),
                     ('fee_label',    'feeLabel',     str,   False),
                     ('fee_description', 'feeDescription', str, False),
                     ('fee_accepts',  'feeAccepts',   str,   False),
                     ('fee_amount',   'feeAmount',    float, False),
                     ('created_date', 'createdDate',  int,   False),
                    ]:
                    event[name] = get_field(subdocument, fieldname, fieldtype,
                                            required=required,
                                            docname='event')
                if non_empty(event):
                    muprofile['events'].append(event)

            elif subdocument['type'] == 'comment':
                comment = {}
                for name, fieldname, fieldtype, required in \
                    [('created_date', 'createdDate', int,   False),
                     ('in_reply_to',  'inReplyTo',   str,   False),
                     ('description',  'description', str,   False),
                     ('url',          'url',         'url', False),
                    ]:
                    comment[name] = get_field(subdocument, fieldname, fieldtype,
                                              required=required,
                                              docname='comment')
                if non_empty(comment):
                    muprofile['comments'].append(comment)

        for subdocument in muprofiledoc.get('other_profiles', []):
            link = {}
            for name, fieldname, fieldtype, required in \
                    [('type',        'type',        str,   True),
                     ('url',         'url',         str,   True),
                    ]:
                link[name] = get_field(subdocument, fieldname, fieldtype,
                                       required=required,
                                       docname='link')
            if non_empty(link):
                muprofile['links'].append(link)

    except FieldError:
        return False

    # add muprofile
    muprofile = dtdb.add_from_dict(muprofile, MUProfile)
    dtdb.commit()
    return True


def add_ghprofile(dtdb, ghprofiledoc, dtsession, logger):
    try:
        check_field(ghprofiledoc, 'sourceId', 'github')
        check_field(ghprofiledoc, 'type', 'profile')
        ghprofile = {}
        for name, fieldname, fieldtype, required in \
            [('profile_id',  'profileId',   str,   True),
             ('crawl_number', 'crawlNumber', int,   True),
             ('name',        'name',        str,   False),
             ('country',     'country',     str,   False),
             ('city',        'city',        str,   False),
             ('company',     'company',     str,   False),
             ('created_date', 'createdDate', int,   False),
             ('profile_url', 'profileUrl',  'url', False),
             ('profile_picture_url', 'profilePictureUrl', 'url', False),
             ('login',       'login',       str,   False),
             ('email',       'email',       str,   False),
             ('contributions_count', 'contributionsCount', int, False),
             ('followers_count', 'followersCount', int, False),
             ('following_count', 'followingCount', int, False),
             ('public_repo_count', 'publicRepoCount', int, False),
             ('public_gist_count', 'publicGistCount', int, False),
             ('indexed_on',  'indexedOn',   int,   True),
             ('crawled_date', 'crawledDate', int,   True),
            ]:
            ghprofile[name] = get_field(ghprofiledoc, fieldname, fieldtype,
                                        required=required)

        ghprofile['repositories']     = []
        ghprofile['links']            = []
        for subdocument in ghprofiledoc.get('sub_documents', []):
            check_field(subdocument, 'type', ['repository'])
            if subdocument['type'] == 'repository':
                repository = {}
                for name, fieldname, fieldtype, required in \
                    [('name',         'name',         str,   False),
                     ('description',  'description',  str,   False),
                     ('full_name',    'fullName',     str,   False),
                     ('url',          'url',          'url', False),
                     ('git_url',      'gitUrl',       str,   False),
                     ('ssh_url',      'sshUrl',       str,   False),
                     ('created_date', 'createdDate',  int,   False),
                     ('pushed_date',  'pushedDate',   int,   False),
                     ('size',         'size',         int,   False),
                     ('default_branch', 'defaultBranch', str,  False),
                     ('view_count',   'viewCount',    int,  False),
                     ('subscribers_count', 'subscribersCount', int,  False),
                     ('forks_count',  'forksCount',   int,  False),
                     ('stargazers_count', 'stargazersCount', int, False),
                     ('open_issues_count', 'openIssuesCount', int, False),
                     ('tags',         'tags',         [str], False),
                    ]:
                    repository[name] = get_field(subdocument, fieldname,
                                                 fieldtype,
                                                 required=required,
                                                 docname='repository')
                if non_empty(repository):
                    ghprofile['repositories'].append(repository)

        for subdocument in ghprofiledoc.get('other_profiles', []):
            link = {}
            for name, fieldname, fieldtype, required in \
                    [('type',        'type',        str,   True),
                     ('url',         'url',         str,   True),
                    ]:
                link[name] = get_field(subdocument, fieldname, fieldtype,
                                      required=required,
                                      docname='link')
            if non_empty(link):
                ghprofile['links'].append(link)

    except FieldError:
        return False

    # add ghprofile
    dtdb.add_from_dict(ghprofile, GHProfile)
    return True


def download_profiles(from_ts, to_ts, maxprofiles, by_indexed_on, source_id):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB(url=conf.DATOIN_DB)
    dtsession = datoin.Session(logger=logger)
    BATCH_SIZE = 1000

    if by_indexed_on:
        from_key = 'fromTs'
        to_key   = 'toTs'
    else:
        from_key = 'crawledFrom'
        to_key   = 'crawledTo'
    params = {from_key : from_ts, to_key : to_ts, 'sid' : source_id}
    if source_id == 'linkedin':
        add_profile = add_liprofile
    elif source_id == 'indeed':
        add_profile = add_inprofile
    elif source_id == 'upwork':
        add_profile = add_uwprofile
    elif source_id == 'meetup':
        add_profile = add_muprofile
    elif source_id == 'github':
        add_profile = add_ghprofile
    else:
        raise ValueError('Invalid source id.')

    logger.log('Downloading profiles from timestamp {0:d} to {1:d}.\n'\
               .format(from_ts, to_ts))
    totalcount = dtsession.count(url=conf.DATOIN3_SEARCH,
                                 params=params)
    if maxprofiles is not None:
        totalcount = min(totalcount, maxprofiles)
    count = 0
    failed_profiles = []
    for profiledoc in dtsession.query(url=conf.DATOIN3_SEARCH,
                                      params=params):
        if 'profileId' not in profiledoc:
            raise IOError('Encountered profile without profileId.')
        profile_id = profiledoc['profileId']
        if 'crawlNumber' not in profiledoc:
            raise IOError('Encountered profile without crawlNumber.')
        crawl_number = profiledoc['crawlNumber']

        if not add_profile(dtdb, profiledoc, dtsession, logger):
            logger.log('Failed profile {0:s}|{1:s}.\n' \
                       .format(str(profile_id), str(crawl_number)))
            failed_profiles.append((profile_id, crawl_number))
        count += 1

        # commit
        if count % BATCH_SIZE == 0:
            logger.log('{0:d} of {1:d} profiles processed ({2:.0f}%).\n' \
                       .format(count, totalcount, count/totalcount*100))
            dtdb.commit()
        if maxprofiles is not None and count >= maxprofiles:
            break
    if count % BATCH_SIZE != 0:
        logger.log('{0:d} profiles processed.\n'.format(count))
    dtdb.commit()

    if count != totalcount:
        raise IOError('Expected {0:d} profiles, recieved {1:d}.' \
                      .format(totalcount, count))

    return count, failed_profiles


def download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on, source_id):
    logger = Logger(sys.stdout)
    njobs = max(njobs, 1)
    if source_id is None:
        logger.log('Downloading LinkedIn profiles.\n')
        download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on,
                       'linkedin', offset=offset, maxoffset=maxoffset)
        logger.log('Downloading Indeed profiles.\n')
        download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on, 'indeed',
                       offset=offset, maxoffset=maxoffset)
        logger.log('Downloading Upwork profiles.\n')
        download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on, 'upwork',
                       offset=offset, maxoffset=maxoffset)
        logger.log('Downloading Meetup profiles.\n')
        download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on, 'meetup',
                       offset=offset, maxoffset=maxoffset)
        logger.log('Downloading GitHub profiles.\n')
        download_range(tfrom, tto, njobs, maxprofiles, by_indexed_on, 'github',
                       offset=offset, maxoffset=maxoffset)
        return

    from_ts = int((tfrom - timestamp0).total_seconds())*1000
    to_ts   = int((tto   - timestamp0).total_seconds())*1000
    timestamps = np.linspace(from_ts, to_ts, njobs+1, dtype=int)

    dlstart = datetime.now()
    logger.log('Downloading time range {0:s} to {1:s}.\n' \
               .format(tfrom.strftime('%Y-%m-%d'),
                       tto.strftime('%Y-%m-%d')))
    logger.log('Starting at {0:s}.\n' \
               .format(dlstart.strftime('%Y-%m-%d %H:%M:%S%z')))

    if njobs > 1:
        args = [(ts1, ts2, maxprofiles, by_indexed_on, source_id) \
                for ts1, ts2 in zip(timestamps[:-1], timestamps[1:])]
        results = ParallelFunction(download_profiles,
                                   batchsize=1,
                                   workdir='jobs',
                                   prefix='lidownload',
                                   tries=1)(args)
        count = 0
        failed_profiles = []
        for c, fp in results:
            count += c
            failed_profiles.extend(fp)
    else:
        count, failed_profiles = download_profiles(from_ts, to_ts, maxprofiles,
                                                 by_indexed_on, source_id)

    dlend = datetime.now()
    dltime = (dlend-dlstart).total_seconds()
    logger.log(dlend.strftime('Finished download %Y-%m-%d %H:%M:%S%z'))
    if dltime > 0:
        logger.log(' at {0:f} profiles/sec.\n' \
                   .format(count/dltime))
    else:
        logger.log('.\n')

    if failed_profiles:
        logger.log('failed profiles:\n')
        for profile_id, crawl_number in failed_profiles:
            logger.log('{0:s}|{0:s}'.format(str(profile_id), str(crawl_number)))

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
    by_indexed_on = bool(args.by_index_date)
    maxprofiles = args.max_profiles
    source_id = args.source
    step_size = args.step_size

    timestamp0 = datetime(year=1970, month=1, day=1)

    deltat = timedelta(days=step_size)
    profilecount = 0
    t = fromdate
    if maxprofiles is None:
        while t < todate:
            profilecount \
                += download_range(t, min(t+deltat, todate), njobs,
                                 None, by_indexed_on, source_id)
            t += deltat
    else:
        while t < todate and profilecount < maxprofiles:
            profilecount \
                += download_range(t, min(t+deltat, todate), njobs,
                                 maxprofiles-profilecount, by_indexed_on,
                                 source_id)
            t += deltat

