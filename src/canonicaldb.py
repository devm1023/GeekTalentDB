__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'LIProfileSkill',
    'LIExperienceSkill',
    'INProfile',
    'INExperience',
    'INEducation',
    'INProfileSkill',
    'INExperienceSkill',
    'UWProfile',
    'UWExperience',
    'UWEducation',
    'UWProfileSkill',
    'UWExperienceSkill',
    'MUProfile',
    'MUProfileSkill',
    'MULink',
    'GHProfile',
    'GHProfileSkill',
    'GHLink',
    'Location',
    'CanonicalDB',
    ]

import conf
import numpy as np
import requests
from copy import deepcopy
from sqldb import *
from textnormalization import *
from logger import Logger
from sqlalchemy import \
    Column, \
    ForeignKey, \
    UniqueConstraint, \
    Integer, \
    BigInteger, \
    Unicode, \
    UnicodeText, \
    String, \
    Text, \
    DateTime, \
    Date, \
    Float, \
    Boolean, \
    func
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from phraseextract import PhraseExtractor
from textnormalization import tokenized_skill
import time
import random
from pprint import pprint


STR_MAX = 100000

SQLBase = sqlbase()


# LinkedIn

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    crawl_number  = Column(BigInteger, index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    is_company    = Column(Boolean)
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    sector        = Column(Unicode(STR_MAX))
    nrm_sector    = Column(Unicode(STR_MAX), index=True)
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    connections   = Column(Integer)
    text_length   = Column(Integer)
    n_experiences = Column(Integer)
    first_experience_start = Column(DateTime)
    last_experience_start  = Column(DateTime)
    curr_title    = Column(Unicode(STR_MAX))
    nrm_curr_title = Column(Unicode(STR_MAX))
    curr_title_prefix = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX), index=True)
    n_educations  = Column(Integer)
    first_education_start  = Column(DateTime)
    last_education_start   = Column(DateTime)
    last_institute = Column(Unicode(STR_MAX))
    nrm_last_institute = Column(Unicode(STR_MAX))
    last_subject = Column(Unicode(STR_MAX))
    nrm_last_subject = Column(Unicode(STR_MAX))
    last_degree = Column(Unicode(STR_MAX))
    nrm_last_degree = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    experiences   = relationship('LIExperience',
                                 order_by='LIExperience.start',
                                 cascade='all, delete-orphan')
    educations    = relationship('LIEducation',
                                 order_by='LIEducation.start',
                                 cascade='all, delete-orphan')
    skills        = relationship('LIProfileSkill',
                                 order_by='LIProfileSkill.nrm_name',
                                 cascade='all, delete-orphan')
    groups        = relationship('LIGroup',
                                 order_by='LIGroup.url',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id            = Column(BigInteger, primary_key=True)
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX), index=True)
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    start         = Column(DateTime)
    end           = Column(DateTime)
    duration      = Column(Integer) # duration in days
    description   = Column(Unicode(STR_MAX))

    skills        = relationship('LIExperienceSkill',
                                 order_by='LIExperienceSkill.skill_id',
                                 cascade='all, delete-orphan')

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id            = Column(BigInteger, primary_key=True)
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    institute     = Column(Unicode(STR_MAX))
    nrm_institute = Column(Unicode(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    nrm_degree    = Column(Unicode(STR_MAX))
    subject       = Column(Unicode(STR_MAX))
    nrm_subject   = Column(Unicode(STR_MAX))
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))

class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id            = Column(BigInteger, primary_key=True)
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX), index=True)

class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    id            = Column(BigInteger, primary_key=True)
    liprofile_id  = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)
    reenforced    = Column(Boolean)
    score         = Column(Float)

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperience_id = Column(BigInteger, ForeignKey('liexperience.id'),
                             primary_key=True,
                             index=True)
    skill_id      = Column(BigInteger, ForeignKey('liprofile_skill.id'),
                           primary_key=True,
                           index=True)
    skill         = relationship('LIProfileSkill')


# Indeed

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    crawl_number  = Column(BigInteger, index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX), index=True)
    description   = Column(Unicode(STR_MAX))
    additional_information = Column(Unicode(STR_MAX))
    text_length   = Column(Integer)
    first_experience_start = Column(DateTime)
    last_experience_start  = Column(DateTime)
    first_education_start  = Column(DateTime)
    last_education_start   = Column(DateTime)
    url           = Column(String(STR_MAX))
    updated_on    = Column(DateTime)
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    experiences   = relationship('INExperience',
                                 order_by='INExperience.start',
                                 cascade='all, delete-orphan')
    educations    = relationship('INEducation',
                                 order_by='INEducation.start',
                                 cascade='all, delete-orphan')
    certifications = relationship('INCertification',
                                  order_by='INCertification.name',
                                  cascade='all, delete-orphan')
    skills        = relationship('INProfileSkill',
                                 order_by='INProfileSkill.nrm_name',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id            = Column(BigInteger, primary_key=True)
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    language      = Column(String(20))
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX), index=True)
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    start         = Column(DateTime)
    end           = Column(DateTime)
    duration      = Column(Integer) # duration in days
    description   = Column(Unicode(STR_MAX))

    skills        = relationship('INExperienceSkill',
                                 order_by='INExperienceSkill.skill_id',
                                 cascade='all, delete-orphan')

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id            = Column(BigInteger, primary_key=True)
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    language      = Column(String(20))
    institute     = Column(Unicode(STR_MAX))
    nrm_institute = Column(Unicode(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    nrm_degree    = Column(Unicode(STR_MAX))
    subject       = Column(Unicode(STR_MAX))
    nrm_subject   = Column(Unicode(STR_MAX))
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))

class INCertification(SQLBase):
    __tablename__ = 'incertification'
    id            = Column(BigInteger, primary_key=True)
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    name          = Column(Unicode(STR_MAX))
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))

class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    id            = Column(BigInteger, primary_key=True)
    inprofile_id  = Column(BigInteger,
                           ForeignKey('inprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)
    reenforced    = Column(Boolean)
    score         = Column(Float)

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    inexperience_id = Column(BigInteger, ForeignKey('inexperience.id'),
                             primary_key=True,
                             index=True)
    skill_id      = Column(BigInteger, ForeignKey('inprofile_skill.id'),
                           primary_key=True,
                           index=True)
    skill         = relationship('INProfileSkill')


# Upwork

class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    crawl_number  = Column(BigInteger, index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    last_name     = Column(Unicode(STR_MAX))
    first_name    = Column(Unicode(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    text_length   = Column(Integer)
    first_experience_start = Column(DateTime)
    last_experience_start  = Column(DateTime)
    first_education_start  = Column(DateTime)
    last_education_start   = Column(DateTime)
    url           = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    experiences   = relationship('UWExperience',
                                 order_by='UWExperience.start',
                                 cascade='all, delete-orphan')
    educations    = relationship('UWEducation',
                                 order_by='UWEducation.start',
                                 cascade='all, delete-orphan')
    tests         = relationship('UWTest',
                                 order_by='UWTest.name',
                                 cascade='all, delete-orphan')
    skills        = relationship('UWProfileSkill',
                                 order_by='UWProfileSkill.nrm_name',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id            = Column(BigInteger, primary_key=True)
    uwprofile_id  = Column(BigInteger,
                            ForeignKey('uwprofile.id'),
                            index=True)
    language      = Column(String(20))
    title         = Column(Unicode(STR_MAX))
    parsed_title  = Column(Unicode(STR_MAX))
    nrm_title     = Column(Unicode(STR_MAX), index=True)
    title_prefix  = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX), index=True)
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    start         = Column(DateTime)
    end           = Column(DateTime)
    duration      = Column(Integer) # duration in days
    description   = Column(Unicode(STR_MAX))

    skills        = relationship('UWExperienceSkill',
                                 order_by='UWExperienceSkill.skill_id',
                                 cascade='all, delete-orphan')

class UWEducation(SQLBase):
    __tablename__ = 'uweducation'
    id            = Column(BigInteger, primary_key=True)
    uwprofile_id  = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           index=True)
    language      = Column(String(20))
    institute     = Column(Unicode(STR_MAX))
    nrm_institute = Column(Unicode(STR_MAX))
    degree        = Column(Unicode(STR_MAX))
    nrm_degree    = Column(Unicode(STR_MAX))
    subject       = Column(Unicode(STR_MAX))
    nrm_subject   = Column(Unicode(STR_MAX))
    start         = Column(DateTime)
    end           = Column(DateTime)
    description   = Column(Unicode(STR_MAX))

class UWTest(SQLBase):
    __tablename__ = 'uwtest'
    id            = Column(BigInteger, primary_key=True)
    uwprofile_id  = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           index=True)
    name          = Column(Unicode(STR_MAX))
    score         = Column(Float)

class UWProfileSkill(SQLBase):
    __tablename__ = 'uwprofile_skill'
    id            = Column(BigInteger, primary_key=True)
    uwprofile_id  = Column(BigInteger,
                           ForeignKey('uwprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)
    reenforced    = Column(Boolean)
    score         = Column(Float)

class UWExperienceSkill(SQLBase):
    __tablename__ = 'uwexperience_skill'
    uwexperience_id = Column(BigInteger, ForeignKey('uwexperience.id'),
                             primary_key=True,
                             index=True)
    skill_id      = Column(BigInteger, ForeignKey('uwprofile_skill.id'),
                           primary_key=True,
                           index=True)
    skill         = relationship('UWProfileSkill')


# Meetup

class MUProfile(SQLBase):
    __tablename__ = 'muprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    crawl_number  = Column(BigInteger, index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))
    status        = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    picture_id    = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    hq_picture_url = Column(String(STR_MAX))
    thumb_picture_url = Column(String(STR_MAX))
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    groups        = relationship('MUGroup',
                                 cascade='all, delete-orphan')
    events        = relationship('MUEvent',
                                 cascade='all, delete-orphan')
    comments      = relationship('MUComment',
                                 cascade='all, delete-orphan')
    skills        = relationship('MUProfileSkill',
                                 order_by='MUProfileSkill.nrm_name',
                                 cascade='all, delete-orphan')
    links         = relationship('MULink',
                                 order_by='MULink.url',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class MUGroup(SQLBase):
    __tablename__ = 'mugroup'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))
    timezone      = Column(Unicode(STR_MAX))
    utc_offset    = Column(Integer)
    name          = Column(Unicode(STR_MAX))
    category_name = Column(Unicode(STR_MAX))
    category_shortname = Column(Unicode(STR_MAX))
    category_id   = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    urlname       = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    picture_id    = Column(BigInteger)
    hq_picture_url = Column(String(STR_MAX))
    thumb_picture_url = Column(String(STR_MAX))
    join_mode     = Column(Unicode(STR_MAX))
    rating        = Column(Float)
    organizer_name = Column(Unicode(STR_MAX))
    organizer_id  = Column(String(STR_MAX))
    members       = Column(Integer)
    state         = Column(Unicode(STR_MAX))
    visibility    = Column(Unicode(STR_MAX))
    who           = Column(Unicode(STR_MAX))
    created_on    = Column(DateTime)

    skills        = relationship('MUGroupSkill',
                                 order_by='MUGroupSkill.nrm_name',
                                 cascade='all, delete-orphan')

class MUEvent(SQLBase):
    __tablename__ = 'muevent'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    address_line1 = Column(Unicode(STR_MAX))
    address_line2 = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))
    phone         = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))
    time          = Column(DateTime)
    utc_offset    = Column(Integer)
    status        = Column(Unicode(STR_MAX))
    headcount     = Column(Integer)
    visibility    = Column(Unicode(STR_MAX))
    rsvp_limit    = Column(Integer)
    yes_rsvp_count = Column(Integer)
    maybe_rsvp_count = Column(Integer)
    waitlist_count = Column(Integer)
    rating_count   = Column(Integer)
    rating_average = Column(Float)
    fee_required   = Column(Unicode(STR_MAX))
    fee_currency   = Column(Unicode(STR_MAX))
    fee_label      = Column(Unicode(STR_MAX))
    fee_description = Column(Unicode(STR_MAX))
    fee_accepts    = Column(Unicode(STR_MAX))
    fee_amount     = Column(Float)
    created_on     = Column(DateTime)

class MUComment(SQLBase):
    __tablename__ = 'mucomment'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    created_on    = Column(DateTime)
    in_reply_to   = Column(String(STR_MAX))
    description   = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class MULink(SQLBase):
    __tablename__ = 'mulink'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

class MUProfileSkill(SQLBase):
    __tablename__ = 'muprofile_skill'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)
    reenforced    = Column(Boolean)

class MUGroupSkill(SQLBase):
    __tablename__ = 'mugroup_skill'
    id            = Column(BigInteger, primary_key=True)
    muprofile_id  = Column(BigInteger,
                           ForeignKey('mugroup.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)


# GitHub

class GHProfile(SQLBase):
    __tablename__ = 'ghprofile'
    id            = Column(BigInteger, primary_key=True)
    datoin_id     = Column(String(STR_MAX), index=True)
    crawl_number  = Column(BigInteger, index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    nrm_location  = Column(Unicode(STR_MAX), index=True)
    company       = Column(Unicode(STR_MAX))
    nrm_company   = Column(Unicode(STR_MAX))
    created_on    = Column(DateTime)
    url           = Column(String(STR_MAX))
    picture_url   = Column(String(STR_MAX))
    login         = Column(String(STR_MAX))
    email         = Column(String(STR_MAX))
    contributions_count = Column(Integer)
    followers_count = Column(Integer)
    following_count = Column(Integer)
    public_repo_count = Column(Integer)
    public_gist_count = Column(Integer)
    indexed_on    = Column(DateTime, index=True)
    crawled_on    = Column(DateTime, index=True)
    crawl_fail_count = Column(BigInteger, index=True)

    skills        = relationship('GHProfileSkill',
                                 order_by='GHProfileSkill.nrm_name',
                                 cascade='all, delete-orphan')
    links         = relationship('GHLink',
                                 order_by='GHLink.url',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('datoin_id'),)

class GHProfileSkill(SQLBase):
    __tablename__ = 'ghprofile_skill'
    id            = Column(BigInteger, primary_key=True)
    ghprofile_id  = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    nrm_name      = Column(Unicode(STR_MAX), index=True)
    score         = Column(Float)

class GHRepository(SQLBase):
    __tablename__ = 'ghrepository'
    id            = Column(BigInteger, primary_key=True)
    ghprofile_id  = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           index=True)
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    full_name     = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    git_url       = Column(String(STR_MAX))
    ssh_url       = Column(String(STR_MAX))
    created_on    = Column(DateTime)
    pushed_on     = Column(DateTime)
    size          = Column(Integer)
    default_branch = Column(String(STR_MAX))
    view_count    = Column(Integer)
    subscribers_count = Column(Integer)
    forks_count   = Column(Integer)
    stargazers_count = Column(Integer)
    open_issues_count = Column(Integer)

    tags          = relationship('GHRepositorySkill')

class GHRepositorySkill(SQLBase):
    __tablename__  = 'ghrepository_skill'
    ghrepository_id = Column(BigInteger,
                             ForeignKey('ghrepository.id'),
                             primary_key=True,
                             index=True)
    skill_id       = Column(BigInteger,
                            ForeignKey('ghprofile_skill.id'),
                            primary_key=True,
                            index=True)
    skill          = relationship('GHProfileSkill')

class GHLink(SQLBase):
    __tablename__ = 'ghlink'
    id            = Column(BigInteger, primary_key=True)
    ghprofile_id  = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))


# Locations

class Location(SQLBase):
    __tablename__ = 'location'
    nrm_name      = Column(Unicode(STR_MAX), primary_key=True)
    name          = Column(Unicode(STR_MAX), index=True)
    place_id      = Column(String(STR_MAX), index=True)
    geo           = Column(Geometry('POINT'))
    minlat        = Column(Float)
    minlon        = Column(Float)
    maxlat        = Column(Float)
    maxlon        = Column(Float)
    tries         = Column(Integer, index=True)
    ambiguous     = Column(Boolean)


def _joinfields(*args):
    return ' '.join([a for a in args if a])

def _get_length(d, *fields):
    if not d:
        return 0
    count = 0
    for field in fields:
        if isinstance(field, list):
            if len(field) == 1:
                field = field[0]
            elif len(field) == 0:
                continue
            elif d.get(field[0], None):
                for subd in d[field[0]]:
                    count += _get_length(subd, field[1:])
                continue
        if isinstance(field, str):
            text = d.get(field, None)
            if text is None:
                continue
            if not isinstance(text, str):
                raise ValueError('Field `{0:s}` of type {1:s} (must be str).' \
                                 .format(field, repr(type(text))))
            count += len(text)
    return count


# LinkedIn

def _make_liexperience(liexperience, language):
    liexperience.pop('id', None)
    liexperience.pop('liprofile_id', None)
    liexperience['language']     = language
    liexperience['parsed_title'] = parsed_title(language,
                                                liexperience['title'])
    liexperience['nrm_title']    = normalized_title('linkedin', language,
                                                    liexperience['title'])
    liexperience['title_prefix'] = normalized_title_prefix(
        language, liexperience['title'])
    liexperience['nrm_company']  = normalized_company(
        'linkedin', language, liexperience['company'])
    liexperience['nrm_location'] = normalized_location(liexperience['location'])

    # work out duration
    liexperience['duration'] = None
    if liexperience['start'] is not None and liexperience['end'] is not None:
        liexperience['duration'] \
            = (liexperience['end'] - liexperience['start']).days

    return liexperience

def _make_lieducation(lieducation, language):
    lieducation.pop('id', None)
    lieducation.pop('liprofile_id', None)
    lieducation['language']     = language
    lieducation['nrm_institute'] = normalized_institute(
        'linkedin', language, lieducation['institute'])
    lieducation['nrm_degree']    = normalized_degree(
        'linkedin', language, lieducation['degree'])
    lieducation['nrm_subject']   = normalized_subject(
        'linkedin', language, lieducation['subject'])

    return lieducation

def _make_liprofile_skill(skillname, language):
    nrm_name = normalized_skill('linkedin', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name,
                'reenforced' : False}

def _make_ligroup(group, language):
    group.pop('id', None)
    group.pop('liprofile_id', None)
    group['language'] = language
    return group

def _is_company(language, name):
    if language != 'en' or not name:
        return False
    tokens = clean(name, lowercase=True, tokenize=True)
    return ('limited' in tokens or 'ltd' in tokens)

def _make_liprofile(liprofile):
    liprofile = deepcopy(liprofile)

    # determine current company
    company = None
    currentexperiences = [e for e in liprofile['experiences'] \
                          if e['start'] is not None and e['end'] is None]
    currentexperiences.sort(key=lambda e: e['start'])
    company = None
    curr_title = None
    if currentexperiences:
        company = currentexperiences[-1]['company']
        curr_title = currentexperiences[-1]['title']
    if liprofile['title']:
        titleparts = liprofile['title'].split(' at ')
        if len(titleparts) > 1:
            if not company:
                company = titleparts[1]
            if not curr_title:
                curr_title = titleparts[0]

    # get profile language
    language = liprofile.get('language', None)

    # normalize fields
    liprofile['nrm_location']     = normalized_location(liprofile['location'])
    liprofile['parsed_title']     = parsed_title(language, liprofile['title'])
    liprofile['nrm_title']        = normalized_title('linkedin', language,
                                                     liprofile['title'])
    liprofile['title_prefix']     = normalized_title_prefix(language,
                                                            liprofile['title'])
    liprofile['curr_title']       = curr_title
    liprofile['nrm_curr_title']   = normalized_title('linkedin', language,
                                                     curr_title)
    liprofile['curr_title_prefix'] = normalized_title_prefix(language,
                                                             curr_title)
    liprofile['nrm_sector']       = normalized_sector(liprofile['sector'])
    liprofile['company']          = company
    liprofile['nrm_company']      = normalized_company('linkedin', language,
                                                       company)

    # tag company profiles
    liprofile['is_company']       = _is_company(language, liprofile['name'])

    # update experiences
    liprofile['experiences'] = [_make_liexperience(e, language) \
                                for e in liprofile['experiences']]
    startdates = [e['start'] for e in liprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        liprofile['first_experience_start'] = min(startdates)
        liprofile['last_experience_start'] = max(startdates)
    else:
        liprofile['first_experience_start'] = None
        liprofile['last_experience_start'] = None

    # update educations
    liprofile['educations'] \
        = [_make_lieducation(e, language) for e in liprofile['educations']]

    # find last education
    educations = liprofile['educations']
    last_education = None
    if len(educations) == 1:
        last_education = educations[0]
    educations = [e for e in educations if e['start'] is not None]
    if last_education is None and educations:
        last_education = max(educations, key=lambda e: e['start'])
    if last_education:
        liprofile['last_institute'] = last_education['institute']
        liprofile['last_subject'] = last_education['subject']
        liprofile['last_degree'] = last_education['degree']
        liprofile['nrm_last_institute'] = normalized_institute(
            'linkedin', language, liprofile['last_institute'])
        liprofile['nrm_last_degree']    = normalized_degree(
            'linkedin', language, liprofile['last_degree'])
        liprofile['nrm_last_subject']   = normalized_subject(
            'linkedin', language, liprofile['last_subject'])
    if educations:
        liprofile['first_education_start'] = min(e['start'] for e in educations)
        liprofile['last_education_start'] = max(e['start'] for e in educations)
    else:
        liprofile['first_education_start'] = None
        liprofile['last_education_start'] = None
    

    # add skills
    liprofile['skills'] = [_make_liprofile_skill(skill, language) \
                           for skill in liprofile['skills']]

    # add groups
    liprofile['groups'] = [_make_ligroup(group, language) \
                           for group in liprofile['groups']]

    # determine text length
    liprofile['text_length'] = _get_length(liprofile, 'title', 'description',
                                           ['experiences', 'title'],
                                           ['experiences', 'description'],
                                           ['skills', 'name'])

    return liprofile


# Indeed

def _make_inexperience(inexperience, language):
    inexperience.pop('id', None)
    inexperience.pop('inprofile_id', None)
    inexperience['language']     = language
    inexperience['parsed_title'] = parsed_title(language, inexperience['title'])
    inexperience['nrm_title']    = normalized_title('indeed', language,
                                                    inexperience['title'])
    inexperience['title_prefix'] = normalized_title_prefix(
        language, inexperience['title'])
    inexperience['nrm_company']  = normalized_company('indeed', language,
                                                      inexperience['company'])
    inexperience['nrm_location'] = normalized_location(
        inexperience['location'])

    # work out duration
    inexperience['duration'] = None
    if inexperience['start'] is not None and inexperience['end'] is not None:
        inexperience['duration'] \
            = (inexperience['end'] - inexperience['start']).days

    return inexperience

def _make_ineducation(ineducation, language):
    ineducation.pop('id', None)
    ineducation.pop('inprofile_id', None)
    ineducation['language']     = language
    ineducation['nrm_institute'] = normalized_institute(
        'indeed', language, ineducation['institute'])
    ineducation['nrm_degree']   = normalized_degree(
        'indeed', language, ineducation['degree'])
    ineducation['nrm_subject']  = normalized_subject(
        'indeed', language, ineducation['subject'])

    return ineducation

def _make_inprofile_skill(skillname, language, reenforced):
    nrm_name = normalized_skill('indeed', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name,
                'reenforced' : reenforced,
                'score'      : 1.0 if reenforced else 0.0}

def _make_inprofile(inprofile):
    inprofile = deepcopy(inprofile)

    # determine current company
    company = None
    currentexperiences = [e for e in inprofile['experiences'] \
                          if e['start'] is not None and e['end'] is None \
                          and e['company']]
    currentexperiences.sort(key=lambda e: e['start'])
    if currentexperiences:
        company = currentexperiences[-1]['company']
    elif inprofile['title']:
        titleparts = inprofile['title'].split(' at ')
        if len(titleparts) > 1:
            company = titleparts[1]

    # get profile language
    language = inprofile.get('language', None)

    # normalize fields
    inprofile['nrm_location']     = normalized_location(inprofile['location'])
    inprofile['parsed_title']     = parsed_title(language, inprofile['title'])
    inprofile['nrm_title']        = normalized_title('indeed', language,
                                                     inprofile['title'])
    inprofile['title_prefix']     = normalized_title_prefix(language,
                                                            inprofile['title'])
    inprofile['company']         = company
    inprofile['nrm_company']      = normalized_company('indeed', language,
                                                       company)

    # update experiences
    inprofile['experiences'] = [_make_inexperience(e, language) \
                                for e in inprofile['experiences']]
    startdates = [e['start'] for e in inprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        inprofile['first_experience_start'] = min(startdates)
        inprofile['last_experience_start'] = max(startdates)
    else:
        inprofile['first_experience_start'] = None
        inprofile['last_experience_start'] = None

    # update educations
    inprofile['educations'] \
        = [_make_ineducation(e, language) for e in inprofile['educations']]
    startdates = [e['start'] for e in inprofile['educations'] \
                  if e['start'] is not None]
    if startdates:
        inprofile['first_education_start'] = min(startdates)
        inprofile['last_education_start'] = max(startdates)
    else:
        inprofile['first_education_start'] = None
        inprofile['last_education_start'] = None

    # update certifications
    for certification in inprofile['certifications']:
        certification.pop('id', None)
        certification.pop('inprofile_id', None)

    # add skills
    profileskills = set(inprofile['skills'])
    allskills = set(inprofile['skills'])
    for inexperience in inprofile['experiences']:
        allskills.update(inexperience.get('skills', []))
    inprofile['skills'] = []
    for skill in allskills:
        inprofile['skills'] \
            .append(_make_inprofile_skill(skill, language,
                                        skill in profileskills))

    # determine text length
    inprofile['text_length'] = _get_length(inprofile, 'title', 'description',
                                         'additional_information',
                                         ['experiences', 'title'],
                                         ['experiences', 'description'],
                                         ['skills', 'name'])

    return inprofile


# Upwork

def _make_uwexperience(uwexperience, language):
    uwexperience = deepcopy(uwexperience)
    uwexperience.pop('id', None)
    uwexperience.pop('uwprofile_id', None)
    uwexperience['language']     = language
    uwexperience['parsed_title'] = parsed_title(language, uwexperience['title'])
    uwexperience['nrm_title']    = normalized_title(
        'upwork', language, uwexperience['title'])
    uwexperience['title_prefix'] = normalized_title_prefix(
        language, uwexperience['title'])
    uwexperience['nrm_company']  = normalized_company(
        'upwork', language, uwexperience['company'])
    uwexperience['nrm_location'] = normalized_location(uwexperience['location'])

    # check start and end dates
    if uwexperience['start'] is not None and uwexperience['end'] is not None:
        if uwexperience['start'] >= uwexperience['end']:
            uwexperience['end'] = None
    if uwexperience['start'] is None:
        uwexperience['end'] = None

    return uwexperience

def _make_uweducation(uweducation, language):
    uweducation = deepcopy(uweducation)
    uweducation.pop('id', None)
    uweducation.pop('uwprofile_id', None)
    uweducation['language']     = language
    uweducation['nrm_institute'] = normalized_institute('upwork', language,
                                                      uweducation['institute'])
    uweducation['nrm_degree']    = normalized_degree('upwork', language,
                                                   uweducation['degree'])
    uweducation['nrm_subject']   = normalized_subject('upwork', language,
                                                    uweducation['subject'])

    if uweducation['start'] is not None and uweducation['end'] is not None:
        if uweducation['start'] >= uweducation['end']:
            uweducation['end'] = None
    if uweducation['start'] is None:
        uweducation['end'] = None

    return uweducation

def _make_uwprofile_skill(skillname, language):
    nrm_name = normalized_skill('upwork', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name,
                'reenforced' : False}

def _make_uwprofile(uwprofile):
    # get profile language
    language = uwprofile.get('language', None)

    # normalize fields
    uwprofile['nrm_location']     = normalized_location(uwprofile['location'])
    uwprofile['parsed_title']     = parsed_title(language, uwprofile['title'])
    uwprofile['nrm_title']        = normalized_title('upwork', language,
                                                   uwprofile['title'])
    uwprofile['title_prefix']     = normalized_title_prefix(language,
                                                         uwprofile['title'])

    # update experiences
    uwprofile['experiences'] = [_make_uwexperience(e, language) \
                                for e in uwprofile['experiences']]
    startdates = [e['start'] for e in uwprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        uwprofile['first_experience_start'] = min(startdates)
        uwprofile['last_experience_start'] = max(startdates)
    else:
        uwprofile['first_experience_start'] = None
        uwprofile['last_experience_start'] = None

    # update educations
    uwprofile['educations'] \
        = [_make_uweducation(e, language) for e in uwprofile['educations']]
    startdates = [e['start'] for e in uwprofile['educations'] \
                  if e['start'] is not None]
    if startdates:
        uwprofile['first_education_start'] = min(startdates)
        uwprofile['last_education_start'] = max(startdates)
    else:
        uwprofile['first_education_start'] = None
        uwprofile['last_education_start'] = None

    # add skills
    uwprofile['skills'] = [_make_uwprofile_skill(skill, language) \
                           for skill in uwprofile['skills']]

    # determine text length
    inprofile['text_length'] = _get_length(inprofile, 'title', 'description',
                                         ['experiences', 'title'],
                                         ['experiences', 'description'],
                                         ['skills', 'name'])

    return uwprofile


# Meetup

def _make_muprofile(muprofile):
    muprofile = deepcopy(muprofile)

    # get profile language
    language = muprofile.get('language', None)

    # add profile skills
    muprofile['skills'] = [_make_muprofile_skill(skill, language) \
                           for skill in muprofile['skills']]

    # update groups
    for group in muprofile['groups']:
        group.pop('id', None)
        group.pop('muprofile_id', None)
        group['skills'] = [_make_mugroup_skill(skill, language) \
                           for skill in group['skills']]

    # update events
    for event in muprofile['events']:
        event.pop('id', None)
        event.pop('muprofile_id', None)

    # update comments
    for comment in muprofile['comments']:
        comment.pop('id', None)
        comment.pop('muprofile_id', None)

    # update links
    for link in muprofile['links']:
        link.pop('id', None)
        link.pop('muprofile_id', None)

    return muprofile

def _make_muprofile_skill(skillname, language):
    nrm_name = normalized_skill('meetup', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name,
                'reenforced' : False}

def _make_mugroup_skill(skillname, language):
    nrm_name = normalized_skill('meetup', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name}


# GitHub

def _make_ghprofile(ghprofile):
    # get profile language
    language = ghprofile.get('language', None)

    # normalize fields
    ghprofile['nrm_location'] = normalized_location(ghprofile['location'])
    ghprofile['nrm_company'] = normalized_company('github', language,
                                                ghprofile['company'])

    # add skills
    skills = set()
    for repo in ghprofile['repositories']:
        if repo['tags']:
            skills.update(repo['tags'])
    ghprofile['skills'] = [_make_ghprofile_skill(skill, language) \
                           for skill in skills]

    return ghprofile

def _make_ghprofile_skill(skillname, language):
    nrm_name = normalized_skill('github', language, skillname)
    if not nrm_name:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrm_name'    : nrm_name,
                'score'      : 0.0}


class GooglePlacesError(Exception):
    pass

class CanonicalDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def rank_skills(self, profile, source):
        if source == 'linkedin':
            experience_skill_tab = LIExperienceSkill
            experience_id_key = 'liexperience_id'
        elif source == 'upwork':
            experience_skill_tab = UWExperienceSkill
            experience_id_key = 'uwexperience_id'
        elif source in ['meetup', 'github']:
            pass
        else:
            raise ValueError('Invalid source type.')

        skill_ids = dict((s.nrm_name, s.id) \
                        for s in profile.skills if s.nrm_name)
        reenforced = dict((s, False) for s in skill_ids.keys())
        scores = dict((s, 0.0) for s in skill_ids.keys())
        tokenize = lambda x: split_nrm_name(x)[-1].split()
        skillextractor = PhraseExtractor(skill_ids.keys(), tokenize=tokenize)
        tokenize = lambda x: tokenized_skill(profile.language, x,
                                            removebrackets=False)

        # extract from profile text
        if source in ['meetup', 'github']:
            profiletext = profile.description
        else:
            profiletext = _joinfields(profile.title, profile.description)
        profileskills = skillextractor(profiletext, tokenize=tokenize)
        profileskills = set(profileskills)
        for skill in profileskills:
            reenforced[skill] = True
            scores[skill] += 1.0

        # extract from experiences
        if source in ['linkedin', 'upwork']:
            for experience in profile.experiences:
                experienceskills \
                    = skillextractor(_joinfields(experience.title,
                                                 experience.description),
                                     tokenize=tokenize)
                experienceskills = set(experienceskills)
                for skill in experienceskills:
                    scores[skill] += 1.0
                    kwargs = {experience_id_key : experience.id,
                              'skill_id'       : skill_ids[skill]}
                    experience.skills.append(experience_skill_tab(**kwargs))

        # update score and reenforced columns
        for skill in profile.skills:
            skill.reenforced = reenforced[skill.nrm_name]
            if source in ['linkedin', 'upwork']:
                skill.score = scores[skill.nrm_name]


    def add_liprofile(self, liprofile):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              datoin_id
              language
              name
              last_name
              first_name
              location
              sector
              title
              company
              description
              connections
              url
              picture_url
              indexed_on
              crawled_on
              experiences (list of dict)
                title
                company
                location
                start
                end
                description
              educations (list of dict)
                institute
                degree
                subject
                start
                end
                description
              skills (list of str)
              groups (list of dict)
                name
                url

        Returns:
          The LIProfile object that was added to the database.

        """
        if liprofile['crawl_fail_count'] > conf.MAX_CRAWL_FAIL_COUNT:
            self.query(LIProfile) \
                .filter(LIProfile.datoin_id == liprofile['datoin_id']) \
                .delete(synchronize_session=False)
            return None

        liprofile_id \
            = self.query(LIProfile.id) \
                  .filter(LIProfile.datoin_id == liprofile['datoin_id']) \
                  .first()
        if liprofile_id is not None:
            liprofile['id'] = liprofile_id[0]
            liexperience_ids \
                = [id for id, in self.query(LIExperience.id) \
                   .filter(LIExperience.liprofile_id == liprofile_id[0])]
            if liexperience_ids:
                self.query(LIExperienceSkill) \
                    .filter(LIExperienceSkill.liexperience_id \
                            .in_(liexperience_ids)) \
                    .delete(synchronize_session=False)
        liprofile = _make_liprofile(liprofile)
        liprofile = self.add_from_dict(liprofile, LIProfile)
        self.flush()
        self.rank_skills(liprofile, 'linkedin')

        return liprofile

    def add_inprofile(self, inprofiledict):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          inprofile (dict): Description of the profile. Must contain the
            following fields:

              datoin_id
              language
              name
              last_name
              first_name
              location
              title
              company
              description
              additional_information
              url
              updated_on
              indexed_on
              crawled_on
              experiences (list of dict)
                title
                company
                location
                start
                end
                description
              education (list of dict)
                institute
                degree
                subject
                start
                end
                description
              skills (list of str)
              certifications (list of dict)
                name
                start
                end
                description

        Returns:
          The INProfile object that was added to the database.

        """
        if inprofiledict['crawl_fail_count'] > conf.MAX_CRAWL_FAIL_COUNT:
            self.query(INProfile) \
                .filter(INProfile.datoin_id == inprofiledict['datoin_id']) \
                .delete(synchronize_session=False)
            return None

        inprofile_id = self.query(INProfile.id) \
                          .filter(INProfile.datoin_id \
                                  == inprofiledict['datoin_id']) \
                          .first()
        if inprofile_id is not None:
            inprofiledict['id'] = inprofile_id[0]
            inexperience_ids \
                = [id for id, in self.query(INExperience.id) \
                   .filter(INExperience.inprofile_id == inprofile_id[0])]
            if inexperience_ids:
                self.query(INExperienceSkill) \
                    .filter(INExperienceSkill.inexperience_id \
                            .in_(inexperience_ids)) \
                    .delete(synchronize_session=False)
        inprofiledict = _make_inprofile(inprofiledict)
        inexperiences = inprofiledict.pop('experiences')
        inprofile = self.add_from_dict(inprofiledict, INProfile)
        self.flush()

        # add experiences and compute skill scores
        skill_ids = dict((s.name, s.id) for s in inprofile.skills)
        scores = dict((s.name, s.score) for s in inprofile.skills)
        for inexperiencedict in inexperiences:
            inexperiencedict['inprofile_id'] = inprofile.id
            skills = []
            for skillname in inexperiencedict.get('skills', []):
                skills.append({'skill_id' : skill_ids[skillname]})
                scores[skillname] += 1.0
            inexperiencedict['skills'] = skills
            self.add_from_dict(inexperiencedict, INExperience)

        # update skill scores
        for skill in inprofile.skills:
            skill.score = scores[skill.name]

        return inprofile

    def add_uwprofile(self, uwprofile):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              datoin_id
              language
              name
              last_name
              first_name
              location
              title
              description
              url
              picture_url
              indexed_on
              crawled_on
              experiences (list of dict)
                title
                company
                location
                start
                end
                description
              education (list of dict)
                institute
                degree
                subject
                start
                end
                description
              tests (list of dict)
                name
                score
              skills (list of str)

        Returns:
          The UWProfile object that was added to the database.

        """
        if liprofile['crawl_fail_count'] > conf.MAX_CRAWL_FAIL_COUNT:
            self.query(UWProfile) \
                .filter(UWProfile.datoin_id == liprofile['datoin_id']) \
                .delete(synchronize_session=False)
            return None

        uwprofile_id \
            = self.query(UWProfile.id) \
                  .filter(UWProfile.datoin_id == uwprofile['datoin_id']) \
                  .first()
        if uwprofile_id is not None:
            uwprofile['id'] = uwprofile_id[0]
            uwexperience_ids \
                = [id for id, in self.query(UWExperience.id) \
                   .filter(UWExperience.uwprofile_id == uwprofile_id[0])]
            if uwexperience_ids:
                self.query(UWExperienceSkill) \
                    .filter(UWExperienceSkill.uwexperience_id \
                            .in_(uwexperience_ids)) \
                    .delete(synchronize_session=False)
        uwprofile = _make_uwprofile(uwprofile)
        uwprofile = self.add_from_dict(uwprofile, UWProfile)
        self.flush()
        self.rank_skills(uwprofile, 'upwork')

        return uwprofile

    def add_muprofile(self, muprofile):
        """Add a Meetup profile to the database (or update if it exists).

        Args:
          muprofile (dict): Description of the profile. Must contain the
            following fields:

              datoin_id
              language
              name
              country
              city
              geo (wkt format)
              status
              description
              url
              picture_id
              picture_url
              hq_picture_url
              thumb_picture_url
              indexed_on
              crawled_on
              groups (list of dict)
                country
                city
                geo (wkt format)
                timezone
                utc_offset
                name
                category_name
                category_shortname
                category_id
                description
                url
                urlname
                picture_url
                picture_id
                hq_picture_url
                thumb_picture_url
                join_mode
                rating
                organizer_name
                organizer_id
                members
                state
                visibility
                who
                created_on
                skills (list of str)
              events (list of dict)
                country
                city
                address_line1
                address_line2
                geo (wkt format)
                phone
                name
                description
                url
                time
                utc_offset
                status
                headcount
                visibility
                rsvp_limit
                yes_rsvp_count
                maybe_rsvp_count
                waitlist_count
                rating_count
                rating_average
                fee_required
                fee_currency
                fee_label
                fee_description
                fee_accepts
                fee_amount
                created_on
              comments (list of dict)
                created_on
                in_reply_to
                description
                url
              links (list of dict)
                type
                url
              skills (list of str)

        Returns:
          The MUProfile object that was added to the database.

        """
        if liprofile['crawl_fail_count'] > conf.MAX_CRAWL_FAIL_COUNT:
            self.query(MUProfile) \
                .filter(MUProfile.datoin_id == liprofile['datoin_id']) \
                .delete(synchronize_session=False)
            return None

        muprofile_id \
            = self.query(MUProfile.id) \
                  .filter(MUProfile.datoin_id == muprofile['datoin_id']) \
                  .first()
        if muprofile_id is not None:
            muprofile['id'] = muprofile_id[0]
        else:
            muprofile.pop('id', None)
        muprofile = _make_muprofile(muprofile)
        muprofile = self.add_from_dict(muprofile, MUProfile)
        self.flush()
        self.rank_skills(muprofile, 'meetup')

        return muprofile

    def add_ghprofile(self, ghprofiledict):
        """Add a GitHub profile to the database (or update if it exists).

        Args:
          ghprofile (dict): Description of the profile. Must contain the
            following fields:

              datoin_id
              language
              name
              location
              company
              created_on
              url
              picture_url
              login
              email
              contributions_count
              followers_count
              following_count
              public_repo_count
              public_gist_count
              indexed_on
              crawled_on
              repositories (list of dict)
                name
                description
                full_name
                url
                git_url
                ssh_url
                created_on
                pushed_on
                size
                default_branch
                view_count
                subscribers_count
                forks_count
                stargazers_count
                open_issues_count
                tags (list of str)

        Returns:
          The GHProfile object that was added to the database.

        """
        if liprofile['crawl_fail_count'] > conf.MAX_CRAWL_FAIL_COUNT:
            self.query(GHProfile) \
                .filter(GHProfile.datoin_id == liprofile['datoin_id']) \
                .delete(synchronize_session=False)
            return None

        ghprofile_id = self.query(GHProfile.id) \
                          .filter(GHProfile.datoin_id == \
                                  ghprofiledict['datoin_id']) \
                          .first()
        if ghprofile_id is not None:
            ghprofiledict['id'] = ghprofile_id[0]
            ghrepository_ids \
                = [id for id, in self.query(GHRepository.id) \
                   .filter(GHRepository.ghprofile_id == ghprofile_id[0])]
            if ghrepository_ids:
                self.query(GHRepositorySkill) \
                    .filter(GHRepositorySkill.ghrepository_id \
                            .in_(ghrepository_ids)) \
                    .delete(synchronize_session=False)
                self.query(GHRepository) \
                    .filter(GHRepository.ghprofile_id == ghprofile_id[0]) \
                    .delete(synchronize_session=False)
        ghprofiledict = _make_ghprofile(ghprofiledict)
        repositories = ghprofiledict.pop('repositories')
        ghprofile = self.add_from_dict(ghprofiledict, GHProfile)
        self.flush()

        skills = dict((skill.name, skill) for skill in ghprofile.skills)
        for repositorydict in repositories:
            tags = repositorydict.pop('tags', None)
            repositorydict['ghprofile_id'] = ghprofile.id
            repository = self.add_from_dict(repositorydict, GHRepository)
            self.flush()
            if tags:
                for tag in tags:
                    self.add(GHRepositorySkill(ghrepository_id=repository.id,
                                               skill_id=skills[tag].id))
                    skills[tag].score += 1.0

        return ghprofile

    def add_location(self, nrm_name, retry=False, logger=Logger(None)):
        """Add a location to the database.

        Args:
          nrm_name (str): The normalized name (via ``normalize_location``) of
            the location.

        Returns:
          The Location object that was added to the database.

        """
        location = self.query(Location) \
                       .filter(Location.nrm_name == nrm_name) \
                       .first()
        if location is not None:
            if not retry or location.place_id is not None:
                return location
        else:
            location = Location(nrm_name=nrm_name, tries=0)
            self.add(location)

        # query Google Places API
        maxattempts = 3
        attempts = 0
        while True:
            attempts += 1
            try:
                r = requests.get(conf.PLACES_API,
                                 params={'key' : conf.PLACES_KEY,
                                         'query' : nrm_name})
            except KeyboardInterrupt:
                raise
            except:
                if attempts > maxattempts:
                    logger.log('Failed request for query {0:s}\n' \
                               .format(repr(nrm_name)))
                    raise
                logger.log('Request failed. Retrying.\n')
                time.sleep(2)
                continue

            url = r.url
            try:
                r = r.json()
            except:
                logger.log('Failed parsing response for URL {0:s}\n' \
                           .format(url))
                r = {'status' : 'ZERO_RESULTS', 'results' : []}

            if 'status' not in r:
                raise GooglePlacesError('Status missing in response.')
            if r['status'] == 'OK':
                if 'results' in r and r['results']:
                    results = r['results']
                    attempts = 0
                    break
                else:
                    msg = 'Received status "OK", but "results" field is absent '
                    'or empty. URL: '+url
                    raise GooglePlacesError(msg)
            elif r['status'] == 'ZERO_RESULTS':
                if 'results' in r and not r['results']:
                    logger.log('No results for URL {0:s}\n'.format(url))
                    results = r['results']
                    attempts = 0
                    break
                else:
                    msg = 'Received status "ZERO_RESULTS", but "results" '
                    'field is absent or non-empty. URL: '+url
                    raise GooglePlacesError(msg)
            elif r['status'] == 'INVALID_REQUEST':
                logger.log('Invalid request for URL {0:s}\n'.format(url))
                results = []
                attempts = 0
                break
            elif r['status'] == 'OVER_QUERY_LIMIT':
                if attempts < maxattempts:
                    logger.log('URL: '+url+'\n')
                    logger.log('Out of quota. Waiting 2 secs.\n')
                    time.sleep(2)
                else:
                    logger.log('URL: '+url+'\n')
                    logger.log('Out of quota. Waiting 3h.\n')
                    attempts = 0
                    time.sleep(3*60*60)
            else:
                if attempts < maxattempts:
                    logger.log('URL: {0:s}\n Unknown status "{0:s}". '
                               'Waiting 2 secs and retrying.\n')
                    time.sleep(2)
                else:
                    logger.log('URL: {0:s}\n Unknown status "{0:s}". '
                               'Giving up.\n')
                    msg = 'Unknown status "'+r['status']+'". URL: '+url
                    raise GooglePlacesError(msg)

        location.tries += 1
        if not results:
            return location
        location.ambiguous = len(results) > 1
        result = results[0]

        # parse result
        lat = result['geometry']['location']['lat']
        lon = result['geometry']['location']['lng']
        pointstr = 'POINT({0:f} {1:f})'.format(lon, lat)
        address = result['formatted_address']
        place_id = result['place_id']
        if 'viewport' in result['geometry']:
            minlat = result['geometry']['viewport']['southwest']['lat']
            minlon = result['geometry']['viewport']['southwest']['lng']
            maxlat = result['geometry']['viewport']['northeast']['lat']
            maxlon = result['geometry']['viewport']['northeast']['lng']
        else:
            minlat = minlon = maxlat = maxlon = None

        # format address
        address = address.split(', ')
        if address:
            for i, s in enumerate(address[:-1]):
                while len(address) > i+1 and address[i+1] == s:
                    del address[i+1]
        address = ', '.join(address)

        # update record
        location.name = address
        location.place_id = place_id
        location.geo = pointstr
        location.minlat = minlat
        location.minlon = minlon
        location.maxlat = maxlat
        location.maxlon = maxlon

        return location
