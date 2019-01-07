from datoindb import *
import canonicaldb as nf
from windowquery import split_process, process_db
from phraseextract import PhraseExtractor
from textnormalization import tokenized_skill
from dbtools import dict_from_row
from sqlalchemy import and_
import conf
import sys
import csv
from datetime import datetime, timedelta
from logger import Logger
import re
import langdetect
from langdetect.lang_detect_exception import LangDetectException
import argparse
from html.parser import HTMLParser

timestamp0 = datetime(year=1970, month=1, day=1)
now = datetime.now()
skillbuttonpatt = re.compile(r'See ([0-9]+\+|Less)')
truncatedpatt = re.compile(r'.*\.\.\.')
connectionspatt = re.compile(r'[^0-9]*([0-9]+)[^0-9]*')
country_languages = {
    'United Kingdom' : 'en',
    'Netherlands'    : 'nl',
    'Nederland'      : 'nl',
    'Germany'        : 'de',
    'Deutschland'    : 'de',
    'Spain'          : 'es',
    #'Finland'        : 'fi',
    'France'         : 'fr',
    'Italy'          : 'it',
    'Italia'         : 'it',
    'Russia'         : 'ru',
    'Россия'         : 'ru',
}

class MLStripper(HTMLParser):
    """Strips HTML from strings """
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def make_name(name, first_name, last_name):
    if name:
        return name
    name = ' '.join(s for s in [first_name, last_name] if s)
    if not name:
        return None
    return name

def make_location(city, country):
    location = ', '.join(s for s in [city, country] if s)
    if not location:
        location = None
    return location

def make_adzuna_location(adzjob):
    locations = list()
    for i in range(5):
        loc_key = 'location{0:d}'.format(i)
        if loc_key in adzjob:
            locations.append(adzjob[loc_key])

    location = ', '.join(l for l in locations)

    return location

def make_date_time_seconds(ts, offset=0):
    if ts:
        result = timestamp0 + timedelta(seconds=ts+offset)
    else:
        result = None
    return result

def make_date_time(ts, offset=0):
    if ts:
        result = timestamp0 + timedelta(milliseconds=ts+offset)
    else:
        result = None
    return result

def make_date_range(ts_from, ts_to, offset=0):
    start = make_date_time(ts_from, offset=offset)
    end = make_date_time(ts_to, offset=offset)
    if start is not None and end is not None and end < start:
        start = None
        end = None
    if start is None:
        end = None

    return start, end

def make_geo(longitude, latitude):
    if longitude is None or latitude is None:
        return None
    return 'POINT({0:f} {1:f})'.format(longitude, latitude)

def make_list(val):
    if not val:
        return []
    else:
        return val

def lastvalid(q):
    currentrow = None
    for row in q:
        if currentrow and row.profile_id != currentrow.profile_id:
            yield currentrow
            currentrow = row
            continue
        if row.crawl_fail_count == 0 \
           or row.crawl_fail_count > conf.MAX_CRAWL_FAIL_COUNT:
            currentrow = row
    if currentrow:
        yield currentrow

def lastvalidjob(q):
    currentrow = None
    for row in q:
        if currentrow and row.id != currentrow.id:
            yield currentrow
            currentrow = row
            continue
        if row.crawl_fail_count == 0 \
           or row.crawl_fail_count > conf.MAX_CRAWL_FAIL_COUNT:
            currentrow = row
    if currentrow:
        yield currentrow

def parse_liprofiles(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(LIProfile).filter(LIProfile.id >= fromid)
    if by_indexed_on:
        q = q.filter(LIProfile.indexed_on >= from_ts,
                     LIProfile.indexed_on < to_ts)
    else:
        q = q.filter(LIProfile.crawled_date >= from_ts,
                     LIProfile.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(LIProfile.id < toid)
    q = q.order_by(LIProfile.profile_id, LIProfile.crawl_number)

    def add_liprofile(liprofile):
        profiledict = dict_from_row(liprofile, pkeys=False, fkeys=False)
        profiledict['datoin_id'] = profiledict.pop('profile_id')
        profiledict['indexed_on'] \
            = make_date_time(profiledict.pop('indexed_on', None))
        profiledict['crawled_on'] \
            = make_date_time(profiledict.pop('crawled_date', None))
        profiledict['name'] = make_name(profiledict.get('name', None),
                                       profiledict.get('first_name', None),
                                       profiledict.get('last_name', None))
        profiledict['location'] \
            = make_location(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profile_url', None)
        profiledict['picture_url'] = profiledict.pop('profile_picture_url', None)

        connections = profiledict.pop('connections', None)
        if connections is not None:
            connections = connectionspatt.match(connections)
        if connections is not None:
            connections = int(connections.group(1))
        profiledict['connections'] = connections

        skills = make_list(profiledict.pop('categories', None))
        skills = [s for s in skills if not skillbuttonpatt.match(s) \
                  and not truncatedpatt.match(s)]
        profiledict['skills'] = skills

        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = make_location(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = make_date_range(experiencedict.pop('date_from', None),
                                experiencedict.pop('date_to', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = make_date_range(educationdict.pop('date_from', None),
                                educationdict.pop('date_to', None))


        # determine language

        profiletexts = [profiledict['title'], profiledict['description']]
        profiletexts.extend(profiledict['skills'])
        for liexperience in profiledict['experiences']:
            profiletexts.append(liexperience['title'])
            profiletexts.append(liexperience['description'])
        for lieducation in profiledict['educations']:
            profiletexts.append(lieducation['degree'])
            profiletexts.append(lieducation['subject'])
            profiletexts.append(lieducation['description'])
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if liprofile.country not in country_languages.keys():
            if language not in country_languages.values():
                return
        elif language not in country_languages.values():
            language = country_languages[liprofile.country]

        profiledict['language'] = language


        # add profile

        cndb.add_liprofile(profiledict)

    process_db(lastvalid(q), add_liprofile, cndb, logger=logger)

def parse_inprofiles(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(INProfile).filter(INProfile.id >= fromid)
    if by_indexed_on:
        q = q.filter(INProfile.indexed_on >= from_ts,
                     INProfile.indexed_on < to_ts)
    else:
        q = q.filter(INProfile.crawled_date >= from_ts,
                     INProfile.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(INProfile.id < toid)
    q = q.order_by(INProfile.profile_id, INProfile.crawl_number)

    def add_inprofile(inprofile):
        profiledict = dict_from_row(inprofile, pkeys=False, fkeys=False)
        profiledict['datoin_id'] = profiledict.pop('profile_id')
        profiledict['indexed_on'] \
            = make_date_time(profiledict.pop('indexed_on', None))
        profiledict['crawled_on'] \
            = make_date_time(profiledict.pop('crawled_date', None))
        profiledict['name'] = make_name(profiledict.get('name', None),
                                       profiledict.get('first_name', None),
                                       profiledict.get('last_name', None))
        profiledict['updated_on'] \
            = make_date_time(profiledict.pop('profile_updated_date', None))
        profiledict['location'] \
            = make_location(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profile_url', None)
        profiledict['skills'] = []

        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = make_location(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = make_date_range(experiencedict.pop('date_from', None),
                                experiencedict.pop('date_to', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = make_date_range(educationdict.pop('date_from', None),
                                educationdict.pop('date_to', None))

        if not profiledict['certifications']:
            profiledict['certifications'] = []
        for certificationdict in profiledict['certifications']:
            certificationdict['start'], certificationdict['end'] \
                = make_date_range(certificationdict.pop('date_from', None),
                                certificationdict.pop('date_to', None))


        # determine language

        profiletexts = [profiledict['title'], profiledict['description']]
        profiletexts.extend(profiledict['skills'])
        for inexperience in profiledict['experiences']:
            profiletexts.append(inexperience['title'])
            profiletexts.append(inexperience['description'])
        for ineducation in profiledict['educations']:
            profiletexts.append(ineducation['degree'])
            profiletexts.append(ineducation['subject'])
            profiletexts.append(ineducation['description'])
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if inprofile.country not in country_languages.keys():
            if language not in country_languages.values():
                return
        elif language not in country_languages.values():
            language = country_languages[inprofile.country]

        profiledict['language'] = language


        # extract skills

        if skillextractors is not None and language in skillextractors:
            text = ' '.join(s for s in [profiledict['title'],
                                        profiledict['description'],
                                        profiledict['additional_information']] \
                            if s)
            profiledict['skills'] = list(set(skillextractors[language](text)))
            for inexperience in profiledict['experiences']:
                text = ' '.join(s for s in [inexperience['title'],
                                            inexperience['description']] if s)
                inexperience['skills'] = list(set(skillextractors[language](text)))


        # add profile

        cndb.add_inprofile(profiledict)

    process_db(lastvalid(q), add_inprofile, cndb, logger=logger)

def parse_adzjobs(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(ADZJob) \
        .filter(ADZJob.id >= fromid)

    q = q.filter(ADZJob.category == category,
                 ADZJob.country == country)

    if by_indexed_on:
        q = q.filter(ADZJob.indexed_on >= from_ts,
                     ADZJob.indexed_on < to_ts)
    else:
        q = q.filter(ADZJob.crawled_date >= from_ts,
                     ADZJob.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(ADZJob.id < toid)
    q = q.order_by(ADZJob.id, ADZJob.category)

    def add_adzjob(adzjob):
        jobdict = dict_from_row(adzjob, pkeys=True, fkeys=True)
        jobdict['adref']            = jobdict.pop('adref')
        jobdict['contract_time']    = jobdict.pop('contract_time')
        jobdict['contract_type']    = jobdict.pop('contract_type')
        jobdict['created']          = jobdict.pop('created')
        jobdict['description']      = jobdict.pop('description')
        jobdict['full_description'] = jobdict.pop('full_description')
        jobdict['adz_id']           = jobdict.pop('adz_id')
        jobdict['latitude']         = jobdict.pop('latitude')
        jobdict['longitude']        = jobdict.pop('longitude')
        jobdict['location_name']    = jobdict.pop('location_name')
        jobdict['redirect_url']     = jobdict.pop('redirect_url')
        jobdict['salary_is_predicted'] = jobdict.pop('salary_is_predicted')
        jobdict['salary_max']       = jobdict.pop('salary_max')
        jobdict['salary_min']       = jobdict.pop('salary_min')
        jobdict['title']            = jobdict.pop('title')
        jobdict['category']         = jobdict.pop('category')
        jobdict['company']          = jobdict.pop('company')
        jobdict['indexed_on'] = make_date_time_seconds(jobdict.pop('indexed_on', None))
        jobdict['crawled_on'] = make_date_time_seconds(jobdict.pop('crawled_date', None))

        jobdict['skills'] = []

        # determine language
        profiletexts = [jobdict['title'], jobdict['full_description']]
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if language not in country_languages.values():
            language = country_languages['United Kingdom'] # Assume English.

        jobdict['language'] = language

        # extract skills
        if jobdict['full_description'] is not None:
            stripped_description = strip_tags(jobdict['full_description'])
        else:
            stripped_description = strip_tags(jobdict['description'])

        if skillextractors is not None and language in skillextractors:
            text = ' '.join(s for s in [jobdict['title'], stripped_description] if s)
            jobdict['skills'] = list(set(skillextractors[language](text)))

        jobdict['crawl_fail_count'] = 0

        # add profile

        cndb.add_adzjob(jobdict)

    process_db(lastvalidjob(q), add_adzjob, cndb, logger=logger)


def parse_uwprofiles(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    s, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(UWProfile).filter(UWProfile.id >= fromid)
    if by_indexed_on:
        q = q.filter(UWProfile.indexed_on >= from_ts,
                     UWProfile.indexed_on < to_ts)
    else:
        q = q.filter(UWProfile.crawled_date >= from_ts,
                     UWProfile.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(UWProfile.id < toid)
    q = q.order_by(UWProfile.profile_id, UWProfile.crawl_number)

    def add_uwprofile(uwprofile):
        profiledict = dict_from_row(uwprofile, pkeys=False, fkeys=False)
        profiledict['datoin_id'] = profiledict.pop('profile_id')
        profiledict['indexed_on'] \
            = make_date_time(profiledict.pop('indexed_on', None))
        profiledict['crawled_on'] \
            = make_date_time(profiledict.pop('crawled_date', None))
        profiledict['name'] = make_name(profiledict.get('name', None),
                                       profiledict.get('first_name', None),
                                       profiledict.get('last_name', None))
        profiledict['location'] \
            = make_location(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profile_url', None)
        profiledict['skills'] = make_list(profiledict.pop('categories', None))

        if not profiledict['experiences']:
            profiledict['experiences'] = []
        for experiencedict in profiledict['experiences']:
            experiencedict['title'] = experiencedict.pop('name', None)
            experiencedict['location'] \
                = make_location(experiencedict.pop('city', None),
                               experiencedict.pop('country', None))
            experiencedict['start'], experiencedict['end'] \
                = make_date_range(experiencedict.pop('date_from', None),
                                experiencedict.pop('date_to', None))

        if not profiledict['educations']:
            profiledict['educations'] = []
        for educationdict in profiledict['educations']:
            educationdict['institute'] = educationdict.pop('name', None)
            educationdict['subject'] = educationdict.pop('area', None)
            educationdict['start'], educationdict['end'] \
                = make_date_range(educationdict.pop('date_from', None),
                                educationdict.pop('date_to', None))

        # determine language
        profiledict['language'] = 'en'

        # add profile
        cndb.add_uwprofile(profiledict)

    process_db(lastvalid(q), add_uwprofile, cndb, logger=logger)

def parse_muprofiles(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(MUProfile).filter(MUProfile.id >= fromid)
    if by_indexed_on:
        q = q.filter(MUProfile.indexed_on >= from_ts,
                     MUProfile.indexed_on < to_ts)
    else:
        q = q.filter(MUProfile.crawled_date >= from_ts,
                     MUProfile.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(MUProfile.id < toid)
    q = q.order_by(MUProfile.profile_id, MUProfile.crawl_number)

    def add_muprofile(muprofile):
        profiledict = dict_from_row(muprofile, pkeys=False, fkeys=False)
        profiledict['datoin_id'] = profiledict.pop('profile_id')
        profiledict['indexed_on'] \
            = make_date_time(profiledict.pop('indexed_on', None))
        profiledict['crawled_on'] \
            = make_date_time(profiledict.pop('crawled_date', None))
        profiledict['picture_id'] \
            = profiledict.pop('profile_picture_id', None)
        profiledict['picture_url'] \
            = profiledict.pop('profile_picture_url', None)
        profiledict['hq_picture_url'] \
            = profiledict.pop('profile_hqpicture_url', None)
        profiledict['thumb_picture_url'] \
            = profiledict.pop('profile_thumb_picture_url', None)
        profiledict['geo'] = make_geo(profiledict.pop('longitude', None),
                                     profiledict.pop('latitude', None))
        profiledict['skills'] = make_list(profiledict.pop('categories', None))

        if not profiledict['groups']:
            profiledict['groups'] = []
        for groupdict in profiledict['groups']:
            groupdict['created_on'] \
                = make_date_time(groupdict.pop('created_date', None))
            groupdict['hq_picture_url'] = groupdict.pop('HQPictureUrl', None)
            groupdict['geo'] = make_geo(groupdict.pop('longitude', None),
                                       groupdict.pop('latitude', None))
            groupdict['skills'] = make_list(groupdict.pop('categories', None))

        if not profiledict['events']:
            profiledict['events'] = []
        for eventdict in profiledict['events']:
            eventdict['created_on'] \
                = make_date_time(eventdict.pop('created_date', None))
            eventdict['time'] = make_date_time(eventdict.get('time', None))
            eventdict['geo'] = make_geo(eventdict.pop('longitude', None),
                                       eventdict.pop('latitude', None))

        profiledict['comments'] = profiledict.pop('comments', None)
        if not profiledict['comments']:
            profiledict['comments'] = []
        for commentdict in profiledict['comments']:
            commentdict['created_on'] \
                = make_date_time(commentdict.pop('created_date', None))

        # determine language
        profiledict['language'] = 'en'

        # add profile
        cndb.add_muprofile(profiledict)

    process_db(lastvalid(q), add_muprofile, cndb, logger=logger)


def parse_ghprofiles(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(GHProfile).filter(GHProfile.id >= fromid)
    if by_indexed_on:
        q = q.filter(GHProfile.indexed_on >= from_ts,
                     GHProfile.indexed_on < to_ts)
    else:
        q = q.filter(GHProfile.crawled_date >= from_ts,
                     GHProfile.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(GHProfile.id < toid)
    q = q.order_by(GHProfile.profile_id, GHProfile.crawl_number)

    def add_ghprofile(ghprofile):
        profiledict = dict_from_row(ghprofile, pkeys=False, fkeys=False)
        profiledict['datoin_id'] = profiledict.pop('profile_id')
        profiledict['indexed_on'] \
            = make_date_time(profiledict.pop('indexed_on', None))
        profiledict['crawled_on'] \
            = make_date_time(profiledict.pop('crawled_date', None))
        profiledict['created_on'] \
            = make_date_time(profiledict.pop('created_date', None))
        profiledict['location'] \
            = make_location(profiledict.pop('city', None),
                           profiledict.pop('country', None))
        profiledict['url'] = profiledict.pop('profile_url', None)
        profiledict['picture_url'] = profiledict.pop('profile_picture_url', None)

        if not profiledict['repositories']:
            profiledict['repositories'] = []
        for repositorydict in profiledict['repositories']:
            repositorydict['created_on'] \
                = make_date_time(repositorydict.pop('created_date', None))
            repositorydict['pushed_on'] \
                = make_date_time(repositorydict.pop('pushed_date', None))

        # determine language
        profiledict['language'] = 'en'

        # add profile
        cndb.add_ghprofile(profiledict)

    process_db(lastvalid(q), add_ghprofile, cndb, logger=logger)



def parse_injobs(jobid, fromid, toid, from_ts, to_ts, by_indexed_on,
                    skillextractors, category, country):
    logger = Logger(sys.stdout)
    dtdb = DatoinDB()
    cndb = nf.CanonicalDB()

    q = dtdb.query(IndeedJob) \
        .filter(IndeedJob.id >= fromid)

    q = q.filter(IndeedJob.category == category,
                 IndeedJob.country == country)

    if by_indexed_on:
        q = q.filter(IndeedJob.indexed_on >= from_ts,
                     IndeedJob.indexed_on < to_ts)
    else:
        q = q.filter(IndeedJob.crawled_date >= from_ts,
                     IndeedJob.crawled_date < to_ts)

    if toid is not None:
        q = q.filter(IndeedJob.id < toid)
    q = q.order_by(IndeedJob.id, IndeedJob.category)

    def add_injob(injob):
        jobdict = dict_from_row(injob, pkeys=True, fkeys=True)
        jobdict['jobkey']           = jobdict.pop('jobkey')
        jobdict['created']          = jobdict.pop('date')
        jobdict['description']      = jobdict.pop('snippet')
        jobdict['full_description'] = ''
        jobdict['latitude']         = jobdict.pop('latitude')
        jobdict['longitude']        = jobdict.pop('longitude')
        jobdict['location_name']    = ', '.join([
            jobdict.pop('formattedLocation'),
            jobdict.pop('state'),
            jobdict.pop('country')
        ])
        jobdict['url']              = jobdict.pop('url')
        jobdict['title']            = jobdict.pop('jobtitle')
        jobdict['category']         = jobdict.pop('category')
        jobdict['company']          = jobdict.pop('company')
        jobdict['indexed_on'] = make_date_time_seconds(jobdict.pop('indexed_on', None))
        jobdict['crawled_on'] = make_date_time_seconds(jobdict.pop('crawled_date', None))

        jobdict['skills'] = []

        # determine language
        profiletexts = [jobdict['title'], jobdict['description']]
        profiletexts = '. '.join([t for t in profiletexts if t])
        try:
            language = langdetect.detect(profiletexts)
        except LangDetectException:
            language = None

        if language not in country_languages.values():
            language = country_languages['United Kingdom'] # Assume English.

        jobdict['language'] = language
        jobdict['crawl_fail_count'] = 0

        cndb.add_injob(jobdict)

    process_db(lastvalidjob(q), add_injob, cndb, logger=logger)

def parse_profiles(njobs, batchsize,
                   from_ts, to_ts, fromid, source_id, by_indexed_on,
                   skillextractors, category, country):
    logger = Logger(sys.stdout)

    # Process all sources if source is not specified.
    if source_id is None:
        parse_profiles(from_ts, to_ts, fromid, 'linkedin', by_indexed_on,
                      skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'indeed', by_indexed_on,
                      skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'upwork', by_indexed_on,
                      skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'meetup', by_indexed_on,
                      skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'github', by_indexed_on,
                      skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'adzuna', by_indexed_on,
                       skillextractors)
        parse_profiles(from_ts, to_ts, fromid, 'indeedjob', by_indexed_on,
                       skillextractors)
        return
    elif source_id == 'linkedin':
        logger.log('Parsing LinkedIn profiles.\n')
        table = LIProfile
        parsefunc = parse_liprofiles
        prefix = 'canonical_parse_linkedin'
    elif source_id == 'indeed':
        logger.log('Parsing Indeed profiles.\n')
        table = INProfile
        parsefunc = parse_inprofiles
        prefix = 'canonical_parse_indeed'
    elif source_id == 'upwork':
        logger.log('Parsing Upwork profiles.\n')
        table = UWProfile
        parsefunc = parse_uwprofiles
        prefix = 'canonical_parse_upwork'
    elif source_id == 'meetup':
        logger.log('Parsing Meetup profiles.\n')
        table = MUProfile
        parsefunc = parse_muprofiles
        prefix = 'canonical_parse_meetup'
    elif source_id == 'github':
        logger.log('Parsing GitHub profiles.\n')
        table = GHProfile
        parsefunc = parse_ghprofiles
        prefix = 'canonical_parse_github'
    elif source_id == 'adzuna':
        logger.log('Parsing Adzuna jobs.\n')
        table = ADZJob
        parsefunc = parse_adzjobs
        prefix = 'canonical_parse_adzuna'
    elif source_id == 'indeedjob':
        logger.log('Parsing Indeed jobs.\n')
        table = IndeedJob
        parsefunc = parse_injobs
        prefix = 'canonical_parse_indeedjob'
    else:
        raise ValueError('Invalid source type.')

    dtdb = DatoinDB()

    query = dtdb.query(table.id)
    if by_indexed_on:
        query = query.filter(table.indexed_on >= from_ts,
                             table.indexed_on < to_ts)
    else:
        query = query.filter(table.crawled_date >= from_ts,
                             table.crawled_date < to_ts)
    if fromid is not None:
        query = query.filter(table.id >= fromid)

    if category is not None and (table == ADZJob or table == IndeedJob):
        query = query.filter(table.category == category)

    if table == ADZJob or table == IndeedJob:
        query = query.filter(table.country == country)

    split_process(query, parsefunc, batchsize,
                  njobs=1,
                  args=[from_ts, to_ts, by_indexed_on, skillextractors, category, country],
                  logger=logger, workdir='jobs', prefix=prefix)


def main(args):
    njobs = max(args.jobs, 1)
    if args.skills is not None:
        print('Skill extraction cannot be run with multiple jobs.')
        njobs = 1

    batchsize = args.batch_size
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
    fromid = args.from_id
    skillfile = args.skills
    source_id = args.source
    category  = args.category
    country = args.country

    from_ts = int((fromdate - timestamp0).total_seconds())*1000
    to_ts   = int((todate   - timestamp0).total_seconds())*1000

    # job timestamps are seconds
    if args.source == 'adzuna' or args.source == 'indeedjob':
        from_ts //= 1000
        to_ts //= 1000

    skillextractors = None
    if skillfile is not None:
        skills = {}
        with open(skillfile, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            for row in csvreader:
                if row:
                    if len(row) == 1:
                        lang = 'en'
                        skill = row[0]
                    else:
                        lang = row[0]
                        skill = row[1]

                    if lang not in skills:
                        skills[lang] = []

                    skills[lang].append(skill)

        skillextractors = {}
        for lang in skills.keys():
            tokenize = lambda x: tokenized_skill(lang, x)

            if source_id == 'adzuna':
                skillextractors[lang] = PhraseExtractor(skills[lang], tokenize=tokenize, margin=2.0, fraction=1.0)
            else:
                skillextractors[lang] = PhraseExtractor(skills[lang], tokenize=tokenize)
        
        del skills

    parse_profiles(njobs, batchsize, from_ts, to_ts, fromid, source_id,
                   by_indexed_on, skillextractors, category, country)
    
    
if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--jobs', type=int, default=1,
                        help='Number of parallel jobs.')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of rows per batch.')
    parser.add_argument('--from-date', help=
                        'Only process profiles crawled or indexed on or after '
                        'this date. Format: YYYY-MM-DD',
                        default='1970-01-01')
    parser.add_argument('--to-date', help=
                        'Only process profiles crawled or indexed before '
                        'this date. Format: YYYY-MM-DD')
    parser.add_argument('--by-index-date', help=
                        'Indicates that the dates specified with --fromdate '
                        'and --todate are index dates. Otherwise they are '
                        'interpreted as crawl dates.',
                        action='store_true')
    parser.add_argument('--from-id', help=
                        'Start processing from this datoin ID. Useful for '
                        'crash recovery.')
    parser.add_argument('--source',
                        choices=['linkedin', 'indeed', 'upwork', 'meetup',
                                 'github', 'adzuna', 'indeedjob'],
                        help=
                        'Source type to process. If not specified all sources '
                        'are processed.')
    parser.add_argument('--skills', help=
                        'Name of a CSV file holding skill tags. Only needed '
                        'when processing Indeed CVs or Adzuna jobs')
    parser.add_argument('--category', help=
                        'Name of an Adzuna category for which skills file contains '
                        'skills. Only needed for jobs')
    parser.add_argument('--country', type=str, default='gb',
                        help='ISO 3166-1 country code. Only supported for jobs.')
    args = parser.parse_args()
    main(args)
