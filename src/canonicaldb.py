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
from textnormalization import tokenizedSkill
import time
import random
from pprint import pprint


STR_MAX = 100000

SQLBase = sqlbase()


# LinkedIn

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    isCompany         = Column(Boolean)
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    sector            = Column(Unicode(STR_MAX))
    nrmSector         = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    connections       = Column(Integer)
    textLength        = Column(Integer)
    firstExperienceStart = Column(DateTime)
    lastExperienceStart  = Column(DateTime)
    firstEducationStart  = Column(DateTime)
    lastEducationStart   = Column(DateTime)
    url               = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    experiences       = relationship('LIExperience',
                                     order_by='LIExperience.start',
                                     cascade='all, delete-orphan')
    educations        = relationship('LIEducation',
                                     order_by='LIEducation.start',
                                     cascade='all, delete-orphan')
    skills            = relationship('LIProfileSkill',
                                     order_by='LIProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    groups            = relationship('LIGroup',
                                     order_by='LIGroup.url',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id             = Column(BigInteger, primary_key=True)
    liprofileId    = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
    language       = Column(String(20))
    title          = Column(Unicode(STR_MAX))
    parsedTitle    = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX), index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    company        = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX), index=True)
    location       = Column(Unicode(STR_MAX))
    nrmLocation    = Column(Unicode(STR_MAX), index=True)
    start          = Column(DateTime)
    end            = Column(DateTime)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('LIExperienceSkill',
                                  order_by='LIExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id          = Column(BigInteger, primary_key=True)
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
    language       = Column(String(20))
    institute      = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX))
    degree         = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX))
    subject        = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX))
    start          = Column(DateTime)
    end            = Column(DateTime)
    description    = Column(Unicode(STR_MAX))

class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id            = Column(BigInteger, primary_key=True)
    liprofileId   = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           index=True)
    language      = Column(String(20))
    name          = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX), index=True)
    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)
    reenforced  = Column(Boolean)
    score       = Column(Float)

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperienceId = Column(BigInteger, ForeignKey('liexperience.id'),
                            primary_key=True,
                            index=True)
    skillId        = Column(BigInteger, ForeignKey('liprofile_skill.id'),
                            primary_key=True,
                            index=True)
    skill          = relationship('LIProfileSkill')


# Indeed
    
class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    additionalInformation = Column(Unicode(STR_MAX))
    textLength        = Column(Integer)
    firstExperienceStart = Column(DateTime)
    lastExperienceStart  = Column(DateTime)
    firstEducationStart  = Column(DateTime)
    lastEducationStart   = Column(DateTime)
    url               = Column(String(STR_MAX))
    updatedOn         = Column(DateTime)
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    experiences       = relationship('INExperience',
                                     order_by='INExperience.start',
                                     cascade='all, delete-orphan')
    educations        = relationship('INEducation',
                                     order_by='INEducation.start',
                                     cascade='all, delete-orphan')
    certifications    = relationship('INCertification',
                                     order_by='INCertification.name',
                                     cascade='all, delete-orphan')
    skills            = relationship('INProfileSkill',
                                     order_by='INProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id             = Column(BigInteger, primary_key=True)
    inprofileId    = Column(BigInteger,
                            ForeignKey('inprofile.id'),
                            index=True)
    language       = Column(String(20))
    title          = Column(Unicode(STR_MAX))
    parsedTitle    = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX), index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    company        = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX), index=True)
    location       = Column(Unicode(STR_MAX))
    nrmLocation    = Column(Unicode(STR_MAX), index=True)
    start          = Column(DateTime)
    end            = Column(DateTime)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('INExperienceSkill',
                                  order_by='INExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id          = Column(BigInteger, primary_key=True)
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         index=True)
    language       = Column(String(20))
    institute      = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX))
    degree         = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX))
    subject        = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX))
    start          = Column(DateTime)
    end            = Column(DateTime)
    description    = Column(Unicode(STR_MAX))

class INCertification(SQLBase):
    __tablename__ = 'incertification'
    id          = Column(BigInteger, primary_key=True)
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    start       = Column(DateTime)
    end         = Column(DateTime)
    description = Column(Unicode(STR_MAX))

class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)
    reenforced  = Column(Boolean)
    score       = Column(Float)

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    inexperienceId = Column(BigInteger, ForeignKey('inexperience.id'),
                            primary_key=True,
                            index=True)
    skillId        = Column(BigInteger, ForeignKey('inprofile_skill.id'),
                            primary_key=True,
                            index=True)
    skill          = relationship('INProfileSkill')


# Upwork

class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    lastName          = Column(Unicode(STR_MAX))
    firstName         = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    textLength        = Column(Integer)
    firstExperienceStart = Column(DateTime)
    lastExperienceStart  = Column(DateTime)
    firstEducationStart  = Column(DateTime)
    lastEducationStart   = Column(DateTime)
    url               = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    experiences       = relationship('UWExperience',
                                     order_by='UWExperience.start',
                                     cascade='all, delete-orphan')
    educations        = relationship('UWEducation',
                                     order_by='UWEducation.start',
                                     cascade='all, delete-orphan')
    tests             = relationship('UWTest',
                                     order_by='UWTest.name',
                                     cascade='all, delete-orphan')
    skills            = relationship('UWProfileSkill',
                                     order_by='UWProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id             = Column(BigInteger, primary_key=True)
    uwprofileId    = Column(BigInteger,
                            ForeignKey('uwprofile.id'),
                            index=True)
    language       = Column(String(20))
    title          = Column(Unicode(STR_MAX))
    parsedTitle    = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX), index=True)
    titlePrefix    = Column(Unicode(STR_MAX))
    company        = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX), index=True)
    location       = Column(Unicode(STR_MAX))
    nrmLocation    = Column(Unicode(STR_MAX), index=True)
    start          = Column(DateTime)
    end            = Column(DateTime)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('UWExperienceSkill',
                                  order_by='UWExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class UWEducation(SQLBase):
    __tablename__ = 'uweducation'
    id          = Column(BigInteger, primary_key=True)
    uwprofileId = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         index=True)
    language       = Column(String(20))
    institute      = Column(Unicode(STR_MAX))
    nrmInstitute   = Column(Unicode(STR_MAX))
    degree         = Column(Unicode(STR_MAX))
    nrmDegree      = Column(Unicode(STR_MAX))
    subject        = Column(Unicode(STR_MAX))
    nrmSubject     = Column(Unicode(STR_MAX))
    start          = Column(DateTime)
    end            = Column(DateTime)
    description    = Column(Unicode(STR_MAX))

class UWTest(SQLBase):
    __tablename__ = 'uwtest'
    id          = Column(BigInteger, primary_key=True)
    uwprofileId = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         index=True)
    name        = Column(Unicode(STR_MAX))
    score       = Column(Float)

class UWProfileSkill(SQLBase):
    __tablename__ = 'uwprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    uwprofileId = Column(BigInteger,
                         ForeignKey('uwprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)
    reenforced  = Column(Boolean)
    score       = Column(Float)

class UWExperienceSkill(SQLBase):
    __tablename__ = 'uwexperience_skill'
    uwexperienceId = Column(BigInteger, ForeignKey('uwexperience.id'),
                            primary_key=True,
                            index=True)
    skillId        = Column(BigInteger, ForeignKey('uwprofile_skill.id'),
                            primary_key=True,
                            index=True)
    skill          = relationship('UWProfileSkill')


# Meetup
    
class MUProfile(SQLBase):
    __tablename__ = 'muprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    country           = Column(Unicode(STR_MAX))
    city              = Column(Unicode(STR_MAX))
    geo               = Column(Geometry('POINT'))
    status            = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    url               = Column(String(STR_MAX))
    pictureId         = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    hqPictureUrl      = Column(String(STR_MAX))
    thumbPictureUrl   = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    groups            = relationship('MUGroup',
                                     cascade='all, delete-orphan')
    events            = relationship('MUEvent',
                                     cascade='all, delete-orphan')
    comments          = relationship('MUComment',
                                     cascade='all, delete-orphan')
    skills            = relationship('MUProfileSkill',
                                     order_by='MUProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    links             = relationship('MULink',
                                     order_by='MULink.url',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class MUGroup(SQLBase):
    __tablename__ = 'mugroup'
    id            = Column(BigInteger, primary_key=True)
    muprofileId   = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))
    timezone      = Column(Unicode(STR_MAX))
    utcOffset     = Column(Integer)
    name          = Column(Unicode(STR_MAX))
    categoryName  = Column(Unicode(STR_MAX))
    categoryShortname = Column(Unicode(STR_MAX))
    categoryId    = Column(Integer)
    description   = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    urlname       = Column(String(STR_MAX))
    pictureUrl    = Column(String(STR_MAX))
    pictureId     = Column(BigInteger)
    hqPictureUrl  = Column(String(STR_MAX))
    thumbPictureUrl = Column(String(STR_MAX))
    joinMode      = Column(Unicode(STR_MAX))
    rating        = Column(Float)
    organizerName = Column(Unicode(STR_MAX))
    organizerId   = Column(String(STR_MAX))
    members       = Column(Integer)
    state         = Column(Unicode(STR_MAX))
    visibility    = Column(Unicode(STR_MAX))
    who           = Column(Unicode(STR_MAX))
    createdOn     = Column(DateTime)

    skills            = relationship('MUGroupSkill',
                                     order_by='MUGroupSkill.nrmName',
                                     cascade='all, delete-orphan')

class MUEvent(SQLBase):
    __tablename__ = 'muevent'
    id            = Column(BigInteger, primary_key=True)
    muprofileId   = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    country       = Column(Unicode(STR_MAX))
    city          = Column(Unicode(STR_MAX))
    addressLine1  = Column(Unicode(STR_MAX))
    addressLine2  = Column(Unicode(STR_MAX))
    geo           = Column(Geometry('POINT'))
    phone         = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))
    time          = Column(DateTime)
    utcOffset     = Column(Integer)
    status        = Column(Unicode(STR_MAX))
    headcount     = Column(Integer)
    visibility    = Column(Unicode(STR_MAX))
    rsvpLimit     = Column(Integer)
    yesRsvpCount  = Column(Integer)
    maybeRsvpCount = Column(Integer)
    waitlistCount = Column(Integer)
    ratingCount   = Column(Integer)
    ratingAverage = Column(Float)
    feeRequired   = Column(Unicode(STR_MAX))
    feeCurrency   = Column(Unicode(STR_MAX))
    feeLabel      = Column(Unicode(STR_MAX))
    feeDescription = Column(Unicode(STR_MAX))
    feeAccepts    = Column(Unicode(STR_MAX))
    feeAmount     = Column(Float)
    createdOn     = Column(DateTime)

class MUComment(SQLBase):
    __tablename__ = 'mucomment'
    id            = Column(BigInteger, primary_key=True)
    muprofileId   = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    createdOn     = Column(DateTime)
    inReplyTo     = Column(String(STR_MAX))
    description   = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))
    
class MULink(SQLBase):
    __tablename__ = 'mulink'
    id            = Column(BigInteger, primary_key=True)
    muprofileId   = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))
    
class MUProfileSkill(SQLBase):
    __tablename__ = 'muprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    muprofileId = Column(BigInteger,
                         ForeignKey('muprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)
    reenforced  = Column(Boolean)

class MUGroupSkill(SQLBase):
    __tablename__ = 'mugroup_skill'
    id          = Column(BigInteger, primary_key=True)
    muprofileId = Column(BigInteger,
                         ForeignKey('mugroup.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)


# GitHub
    
class GHProfile(SQLBase):
    __tablename__ = 'ghprofile'
    id                     = Column(BigInteger, primary_key=True)
    datoinId               = Column(String(STR_MAX), index=True)
    language               = Column(String(20))
    name                   = Column(Unicode(STR_MAX))
    location               = Column(Unicode(STR_MAX))
    nrmLocation            = Column(Unicode(STR_MAX), index=True)
    company                = Column(Unicode(STR_MAX))
    nrmCompany             = Column(Unicode(STR_MAX))
    createdOn              = Column(DateTime)
    url                    = Column(String(STR_MAX))
    pictureUrl             = Column(String(STR_MAX))
    login                  = Column(String(STR_MAX))
    email                  = Column(String(STR_MAX))
    contributionsCount     = Column(Integer)
    followersCount         = Column(Integer)
    followingCount         = Column(Integer)
    publicRepoCount        = Column(Integer)
    publicGistCount        = Column(Integer)
    indexedOn              = Column(DateTime, index=True)
    crawledOn              = Column(DateTime, index=True)

    skills            = relationship('GHProfileSkill',
                                     order_by='GHProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    links             = relationship('GHLink',
                                     order_by='GHLink.url',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class GHProfileSkill(SQLBase):
    __tablename__ = 'ghprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    ghprofileId = Column(BigInteger,
                         ForeignKey('ghprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)
    score       = Column(Float)

class GHRepository(SQLBase):
    __tablename__ = 'ghrepository'
    id            = Column(BigInteger, primary_key=True)
    ghprofileId     = Column(BigInteger,
                             ForeignKey('ghprofile.id'),
                             index=True)    
    name            = Column(Unicode(STR_MAX))
    description     = Column(Unicode(STR_MAX))
    fullName        = Column(Unicode(STR_MAX))
    url             = Column(String(STR_MAX))
    gitUrl          = Column(String(STR_MAX))
    sshUrl          = Column(String(STR_MAX))
    createdOn       = Column(DateTime)
    pushedOn        = Column(DateTime)
    size            = Column(Integer)
    defaultBranch   = Column(String(STR_MAX))
    viewCount       = Column(Integer)
    subscribersCount = Column(Integer)
    forksCount      = Column(Integer)
    stargazersCount = Column(Integer)
    openIssuesCount = Column(Integer)

    tags            = relationship('GHRepositorySkill')    

class GHRepositorySkill(SQLBase):
    __tablename__  = 'ghrepository_skill'
    ghrepositoryId = Column(BigInteger,
                            ForeignKey('ghrepository.id'),
                            primary_key=True,
                            index=True)
    skillId        = Column(BigInteger,
                            ForeignKey('ghprofile_skill.id'),
                            primary_key=True,
                            index=True)
    skill          = relationship('GHProfileSkill')
    
class GHLink(SQLBase):
    __tablename__ = 'ghlink'
    id            = Column(BigInteger, primary_key=True)
    ghprofileId   = Column(BigInteger,
                           ForeignKey('ghprofile.id'),
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))


# Locations
    
class Location(SQLBase):
    __tablename__ = 'location'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX), index=True)
    placeId   = Column(String(STR_MAX), index=True)
    geo       = Column(Geometry('POINT'))
    minlat    = Column(Float)
    minlon    = Column(Float)
    maxlat    = Column(Float)
    maxlon    = Column(Float)
    tries     = Column(Integer, index=True)
    ambiguous = Column(Boolean)


def _joinfields(*args):
    return ' '.join([a for a in args if a])

def _getLength(d, *fields):
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
                    count += _getLength(subd, field[1:])
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

def _makeLIExperience(liexperience, language):
    liexperience.pop('id', None)
    liexperience.pop('liprofileId', None)
    liexperience['language']     = language
    liexperience['parsedTitle']  = parsedTitle(language, liexperience['title'])
    liexperience['nrmTitle']     = normalizedTitle('linkedin', language,
                                                   liexperience['title'])
    liexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                         liexperience['title'])
    liexperience['nrmCompany']   = normalizedCompany('linkedin', language,
                                                     liexperience['company'])
    liexperience['nrmLocation']  = normalizedLocation(liexperience['location'])

    # work out duration
    liexperience['duration'] = None        
    if liexperience['start'] is not None and liexperience['end'] is not None:
        liexperience['duration'] \
            = (liexperience['end'] - liexperience['start']).days

    return liexperience

def _makeLIEducation(lieducation, language):
    lieducation.pop('id', None)
    lieducation.pop('liprofileId', None)
    lieducation['language']     = language
    lieducation['nrmInstitute'] = normalizedInstitute('linkedin', language,
                                                      lieducation['institute'])
    lieducation['nrmDegree']    = normalizedDegree('linkedin', language,
                                                   lieducation['degree'])
    lieducation['nrmSubject']   = normalizedSubject('linkedin', language,
                                                    lieducation['subject'])
    
    return lieducation

def _makeLIProfileSkill(skillname, language):
    nrmName = normalizedSkill('linkedin', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

def _makeLIGroup(group, language):
    group.pop('id', None)
    group.pop('liprofileId', None)
    group['language'] = language
    return group
    
def _isCompany(language, name):
    if language != 'en' or not name:
        return False
    tokens = clean(name, lowercase=True, tokenize=True)
    return ('limited' in tokens or 'ltd' in tokens)
    
def _makeLIProfile(liprofile):
    liprofile = deepcopy(liprofile)
    
    # determine current company
    company = None
    currentexperiences = [e for e in liprofile['experiences'] \
                          if e['start'] is not None and e['end'] is None \
                          and e['company']]
    currentexperiences.sort(key=lambda e: e['start'])
    if currentexperiences:
        company = currentexperiences[-1]['company']
    elif liprofile['title']:
        titleparts = liprofile['title'].split(' at ')
        if len(titleparts) > 1:
            company = titleparts[1]

    # get profile language
    language = liprofile.get('language', None)

    # normalize fields
    liprofile['nrmLocation']     = normalizedLocation(liprofile['location'])
    liprofile['parsedTitle']     = parsedTitle(language, liprofile['title'])
    liprofile['nrmTitle']        = normalizedTitle('linkedin', language,
                                                   liprofile['title'])
    liprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         liprofile['title'])
    liprofile['nrmSector']       = normalizedSector(liprofile['sector'])
    liprofile['company']         = company
    liprofile['nrmCompany']      = normalizedCompany('linkedin', language,
                                                     company)

    # tag company profiles
    liprofile['isCompany']       = _isCompany(language, liprofile['name'])
    
    # update experiences
    liprofile['experiences'] = [_makeLIExperience(e, language) \
                                for e in liprofile['experiences']]
    startdates = [e['start'] for e in liprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        liprofile['firstExperienceStart'] = min(startdates)
        liprofile['lastExperienceStart'] = max(startdates)
    else:
        liprofile['firstExperienceStart'] = None
        liprofile['lastExperienceStart'] = None    

    # update educations
    liprofile['educations'] \
        = [_makeLIEducation(e, language) for e in liprofile['educations']]
    startdates = [e['start'] for e in liprofile['educations'] \
                  if e['start'] is not None]
    if startdates:
        liprofile['firstEducationStart'] = min(startdates)
        liprofile['lastEducationStart'] = max(startdates)
    else:
        liprofile['firstEducationStart'] = None
        liprofile['lastEducationStart'] = None    

    # add skills
    liprofile['skills'] = [_makeLIProfileSkill(skill, language) \
                           for skill in liprofile['skills']]

    # add groups
    liprofile['groups'] = [_makeLIGroup(group, language) \
                           for group in liprofile['groups']]

    # determine text length
    liprofile['textLength'] = _getLength(liprofile, 'title', 'description',
                                         ['experiences', 'title'],
                                         ['experiences', 'description'],
                                         ['skills', 'name'])

    return liprofile


# Indeed

def _makeINExperience(inexperience, language):
    inexperience.pop('id', None)
    inexperience.pop('inprofileId', None)
    inexperience['language']     = language
    inexperience['parsedTitle']  = parsedTitle(language, inexperience['title'])
    inexperience['nrmTitle']     = normalizedTitle('indeed', language,
                                                   inexperience['title'])
    inexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                         inexperience['title'])
    inexperience['nrmCompany']   = normalizedCompany('indeed', language,
                                                     inexperience['company'])
    inexperience['nrmLocation']  = normalizedLocation(inexperience['location'])

    # work out duration
    inexperience['duration'] = None
    if inexperience['start'] is not None and inexperience['end'] is not None:
        inexperience['duration'] \
            = (inexperience['end'] - inexperience['start']).days
        
    return inexperience

def _makeINEducation(ineducation, language):
    ineducation.pop('id', None)
    ineducation.pop('inprofileId', None)
    ineducation['language']     = language
    ineducation['nrmInstitute'] = normalizedInstitute('indeed', language,
                                                      ineducation['institute'])
    ineducation['nrmDegree']    = normalizedDegree('indeed', language,
                                                   ineducation['degree'])
    ineducation['nrmSubject']   = normalizedSubject('indeed', language,
                                                    ineducation['subject'])
    
    return ineducation

def _makeINProfileSkill(skillname, language, reenforced):
    nrmName = normalizedSkill('indeed', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : reenforced,
                'score'      : 1.0 if reenforced else 0.0}

def _makeINProfile(inprofile):
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
    inprofile['nrmLocation']     = normalizedLocation(inprofile['location'])
    inprofile['parsedTitle']     = parsedTitle(language, inprofile['title'])
    inprofile['nrmTitle']        = normalizedTitle('indeed', language,
                                                   inprofile['title'])
    inprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         inprofile['title'])
    inprofile['company']         = company
    inprofile['nrmCompany']      = normalizedCompany('indeed', language,
                                                     company)
    
    # update experiences
    inprofile['experiences'] = [_makeINExperience(e, language) \
                                for e in inprofile['experiences']]
    startdates = [e['start'] for e in inprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        inprofile['firstExperienceStart'] = min(startdates)
        inprofile['lastExperienceStart'] = max(startdates)
    else:
        inprofile['firstExperienceStart'] = None
        inprofile['lastExperienceStart'] = None    

    # update educations
    inprofile['educations'] \
        = [_makeINEducation(e, language) for e in inprofile['educations']]
    startdates = [e['start'] for e in inprofile['educations'] \
                  if e['start'] is not None]
    if startdates:
        inprofile['firstEducationStart'] = min(startdates)
        inprofile['lastEducationStart'] = max(startdates)
    else:
        inprofile['firstEducationStart'] = None
        inprofile['lastEducationStart'] = None

    # update certifications
    for certification in inprofile['certifications']:
        certification.pop('id', None)
        certification.pop('inprofileId', None)

    # add skills
    profileskills = set(inprofile['skills'])
    allskills = set(inprofile['skills'])
    for inexperience in inprofile['experiences']:
        allskills.update(inexperience.get('skills', []))
    inprofile['skills'] = []
    for skill in allskills:
        inprofile['skills'] \
            .append(_makeINProfileSkill(skill, language,
                                        skill in profileskills))

    # determine text length
    inprofile['textLength'] = _getLength(inprofile, 'title', 'description',
                                         'additionalInformation',
                                         ['experiences', 'title'],
                                         ['experiences', 'description'],
                                         ['skills', 'name'])
        
    return inprofile


# Upwork

def _makeUWExperience(uwexperience, language):
    uwexperience = deepcopy(uwexperience)
    uwexperience.pop('id', None)
    uwexperience.pop('uwprofileId', None)
    uwexperience['language']     = language
    uwexperience['parsedTitle']  = parsedTitle(language, uwexperience['title'])
    uwexperience['nrmTitle']     = normalizedTitle('upwork', language,
                                                   uwexperience['title'])
    uwexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                         uwexperience['title'])
    uwexperience['nrmCompany']   = normalizedCompany('upwork', language,
                                                     uwexperience['company'])
    uwexperience['nrmLocation']  = normalizedLocation(uwexperience['location'])

    # check start and end dates
    if uwexperience['start'] is not None and uwexperience['end'] is not None:
        if uwexperience['start'] >= uwexperience['end']:
            uwexperience['end'] = None
    if uwexperience['start'] is None:
        uwexperience['end'] = None

    return uwexperience

def _makeUWEducation(uweducation, language):
    uweducation = deepcopy(uweducation)
    uweducation.pop('id', None)
    uweducation.pop('uwprofileId', None)
    uweducation['language']     = language
    uweducation['nrmInstitute'] = normalizedInstitute('upwork', language,
                                                      uweducation['institute'])
    uweducation['nrmDegree']    = normalizedDegree('upwork', language,
                                                   uweducation['degree'])
    uweducation['nrmSubject']   = normalizedSubject('upwork', language,
                                                    uweducation['subject'])

    if uweducation['start'] is not None and uweducation['end'] is not None:
        if uweducation['start'] >= uweducation['end']:
            uweducation['end'] = None
    if uweducation['start'] is None:
        uweducation['end'] = None
    
    return uweducation

def _makeUWProfileSkill(skillname, language):
    nrmName = normalizedSkill('upwork', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

def _makeUWProfile(uwprofile):
    # get profile language
    language = uwprofile.get('language', None)

    # normalize fields
    uwprofile['nrmLocation']     = normalizedLocation(uwprofile['location'])
    uwprofile['parsedTitle']     = parsedTitle(language, uwprofile['title'])
    uwprofile['nrmTitle']        = normalizedTitle('upwork', language,
                                                   uwprofile['title'])
    uwprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         uwprofile['title'])
    
    # update experiences
    uwprofile['experiences'] = [_makeUWExperience(e, language) \
                                for e in uwprofile['experiences']]
    startdates = [e['start'] for e in uwprofile['experiences'] \
                  if e['start'] is not None]
    if startdates:
        uwprofile['firstExperienceStart'] = min(startdates)
        uwprofile['lastExperienceStart'] = max(startdates)
    else:
        uwprofile['firstExperienceStart'] = None
        uwprofile['lastExperienceStart'] = None    

    # update educations
    uwprofile['educations'] \
        = [_makeUWEducation(e, language) for e in uwprofile['educations']]
    startdates = [e['start'] for e in uwprofile['educations'] \
                  if e['start'] is not None]
    if startdates:
        uwprofile['firstEducationStart'] = min(startdates)
        uwprofile['lastEducationStart'] = max(startdates)
    else:
        uwprofile['firstEducationStart'] = None
        uwprofile['lastEducationStart'] = None    

    # add skills
    uwprofile['skills'] = [_makeUWProfileSkill(skill, language) \
                           for skill in uwprofile['skills']]

    # determine text length
    inprofile['textLength'] = _getLength(inprofile, 'title', 'description',
                                         ['experiences', 'title'],
                                         ['experiences', 'description'],
                                         ['skills', 'name'])
    
    return uwprofile


# Meetup

def _makeMUProfile(muprofile):
    muprofile = deepcopy(muprofile)
    
    # get profile language
    language = muprofile.get('language', None)

    # add profile skills
    muprofile['skills'] = [_makeMUProfileSkill(skill, language) \
                           for skill in muprofile['skills']]

    # update groups
    for group in muprofile['groups']:
        group.pop('id', None)
        group.pop('muprofileId', None)
        group['skills'] = [_makeMUGroupSkill(skill, language) \
                           for skill in group['skills']]

    # update events
    for event in muprofile['events']:
        event.pop('id', None)
        event.pop('muprofileId', None)

    # update comments
    for comment in muprofile['comments']:
        comment.pop('id', None)
        comment.pop('muprofileId', None)

    # update links
    for link in muprofile['links']:
        link.pop('id', None)
        link.pop('muprofileId', None)
        
    return muprofile

def _makeMUProfileSkill(skillname, language):
    nrmName = normalizedSkill('meetup', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

def _makeMUGroupSkill(skillname, language):
    nrmName = normalizedSkill('meetup', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName}
    

# GitHub
    
def _makeGHProfile(ghprofile):
    # get profile language
    language = ghprofile.get('language', None)

    # normalize fields
    ghprofile['nrmLocation'] = normalizedLocation(ghprofile['location'])
    ghprofile['nrmCompany'] = normalizedCompany('github', language,
                                                ghprofile['company'])

    # add skills
    skills = set()
    for repo in ghprofile['repositories']:
        if repo['tags']:
            skills.update(repo['tags'])
    ghprofile['skills'] = [_makeGHProfileSkill(skill, language) \
                           for skill in skills]

    return ghprofile

def _makeGHProfileSkill(skillname, language):
    nrmName = normalizedSkill('github', language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'score'      : 0.0}
    

class GooglePlacesError(Exception):
    pass

class CanonicalDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)        
    
    def rankSkills(self, profile, source):
        if source == 'linkedin':
            experienceSkillTab = LIExperienceSkill
            experienceIdKey = 'liexperienceId'
        elif source == 'upwork':
            experienceSkillTab = UWExperienceSkill
            experienceIdKey = 'uwexperienceId'
        elif source in ['meetup', 'github']:
            pass
        else:
            raise ValueError('Invalid source type.')
        
        skillIds = dict((s.nrmName, s.id) \
                        for s in profile.skills if s.nrmName)
        reenforced = dict((s, False) for s in skillIds.keys())
        scores = dict((s, 0.0) for s in skillIds.keys())
        tokenize = lambda x: splitNrmName(x)[-1].split()
        skillextractor = PhraseExtractor(skillIds.keys(), tokenize=tokenize)
        tokenize = lambda x: tokenizedSkill(profile.language, x,
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
                    kwargs = {experienceIdKey : experience.id,
                              'skillId'       : skillIds[skill]}
                    experience.skills.append(experienceSkillTab(**kwargs))

        # update score and reenforced columns
        for skill in profile.skills:
            skill.reenforced = reenforced[skill.nrmName]
            if source in ['linkedin', 'upwork']:
                skill.score = scores[skill.nrmName]

                
    def addLIProfile(self, liprofile):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              datoinId
              language
              name
              lastName
              firstName
              location
              sector
              title
              company
              description
              connections
              url
              pictureUrl
              indexedOn
              crawledOn
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
        liprofileId = self.query(LIProfile.id) \
                          .filter(LIProfile.datoinId == liprofile['datoinId']) \
                          .first()
        if liprofileId is not None:
            liprofile['id'] = liprofileId[0]
            liexperienceIds \
                = [id for id, in self.query(LIExperience.id) \
                   .filter(LIExperience.liprofileId == liprofileId[0])]
            if liexperienceIds:
                self.query(LIExperienceSkill) \
                    .filter(LIExperienceSkill.liexperienceId \
                            .in_(liexperienceIds)) \
                    .delete(synchronize_session=False)
        liprofile = _makeLIProfile(liprofile)
        liprofile = self.addFromDict(liprofile, LIProfile)
        self.flush()
        self.rankSkills(liprofile, 'linkedin')

        return liprofile

    def addINProfile(self, inprofiledict):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          inprofile (dict): Description of the profile. Must contain the
            following fields:

              datoinId
              language
              name
              lastName
              firstName
              location
              title
              company
              description
              additionalInformation
              url
              updatedOn
              indexedOn
              crawledOn
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
        inprofileId = self.query(INProfile.id) \
                          .filter(INProfile.datoinId \
                                  == inprofiledict['datoinId']) \
                          .first()
        if inprofileId is not None:
            inprofiledict['id'] = inprofileId[0]
            inexperienceIds \
                = [id for id, in self.query(INExperience.id) \
                   .filter(INExperience.inprofileId == inprofileId[0])]
            if inexperienceIds:
                self.query(INExperienceSkill) \
                    .filter(INExperienceSkill.inexperienceId \
                            .in_(inexperienceIds)) \
                    .delete(synchronize_session=False)
        inprofiledict = _makeINProfile(inprofiledict)
        inexperiences = inprofiledict.pop('experiences')
        inprofile = self.addFromDict(inprofiledict, INProfile)
        self.flush()

        # add experiences and compute skill scores
        skillIds = dict((s.name, s.id) for s in inprofile.skills)
        scores = dict((s.name, s.score) for s in inprofile.skills)
        for inexperiencedict in inexperiences:
            inexperiencedict['inprofileId'] = inprofile.id
            skills = []
            for skillname in inexperiencedict.get('skills', []):
                skills.append({'skillId' : skillIds[skillname]})
                scores[skillname] += 1.0
            inexperiencedict['skills'] = skills
            self.addFromDict(inexperiencedict, INExperience)

        # update skill scores
        for skill in inprofile.skills:
            skill.score = scores[skill.name]

        return inprofile

    def addUWProfile(self, uwprofile):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              datoinId
              language
              name
              lastName
              firstName
              location
              title
              description
              url
              pictureUrl
              indexedOn
              crawledOn
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
        uwprofileId = self.query(UWProfile.id) \
                          .filter(UWProfile.datoinId == uwprofile['datoinId']) \
                          .first()
        if uwprofileId is not None:
            uwprofile['id'] = uwprofileId[0]
            uwexperienceIds \
                = [id for id, in self.query(UWExperience.id) \
                   .filter(UWExperience.uwprofileId == uwprofileId[0])]
            if uwexperienceIds:
                self.query(UWExperienceSkill) \
                    .filter(UWExperienceSkill.uwexperienceId \
                            .in_(uwexperienceIds)) \
                    .delete(synchronize_session=False)
        uwprofile = _makeUWProfile(uwprofile)
        uwprofile = self.addFromDict(uwprofile, UWProfile)
        self.flush()
        self.rankSkills(uwprofile, 'upwork')

        return uwprofile

    def addMUProfile(self, muprofile):
        """Add a Meetup profile to the database (or update if it exists).

        Args:
          muprofile (dict): Description of the profile. Must contain the
            following fields:

              datoinId
              language
              name
              country
              city
              geo (wkt format)
              status
              description
              url
              pictureId
              pictureUrl
              hqPictureUrl
              thumbPictureUrl
              indexedOn
              crawledOn
              groups (list of dict)
                country
                city
                geo (wkt format)
                timezone
                utcOffset
                name
                categoryName
                categoryShortname
                categoryId
                description
                url
                urlname
                pictureUrl
                pictureId
                hqPictureUrl
                thumbPictureUrl
                joinMode
                rating
                organizerName
                organizerId
                members
                state
                visibility
                who
                createdOn
                skills (list of str)
              events (list of dict)
                country
                city
                addressLine1
                addressLine2
                geo (wkt format)
                phone
                name
                description
                url
                time
                utcOffset
                status
                headcount
                visibility
                rsvpLimit
                yesRsvpCount
                maybeRsvpCount
                waitlistCount
                ratingCount
                ratingAverage
                feeRequired
                feeCurrency
                feeLabel
                feeDescription
                feeAccepts
                feeAmount
                createdOn
              comments (list of dict)
                createdOn
                inReplyTo
                description
                url
              links (list of dict)
                type
                url
              skills (list of str)

        Returns:
          The MUProfile object that was added to the database.

        """
        muprofileId = self.query(MUProfile.id) \
                          .filter(MUProfile.datoinId == muprofile['datoinId']) \
                          .first()
        if muprofileId is not None:
            muprofile['id'] = muprofileId[0]
        else:
            muprofile.pop('id', None)
        muprofile = _makeMUProfile(muprofile)
        muprofile = self.addFromDict(muprofile, MUProfile)
        self.flush()
        self.rankSkills(muprofile, 'meetup')

        return muprofile
    
    def addGHProfile(self, ghprofiledict):
        """Add a GitHub profile to the database (or update if it exists).

        Args:
          ghprofile (dict): Description of the profile. Must contain the
            following fields:

              datoinId
              language
              name
              location
              company
              createdOn
              url
              pictureUrl
              login
              email
              contributionsCount
              followersCount
              followingCount
              publicRepoCount
              publicGistCount
              indexedOn
              crawledOn
              repositories (list of dict)
                name
                description
                fullName
                url
                gitUrl
                sshUrl
                createdOn
                pushedOn
                size
                defaultBranch
                viewCount
                subscribersCount
                forksCount
                stargazersCount
                openIssuesCount
                tags (list of str)

        Returns:
          The GHProfile object that was added to the database.

        """
        ghprofileId = self.query(GHProfile.id) \
                          .filter(GHProfile.datoinId == \
                                  ghprofiledict['datoinId']) \
                          .first()
        if ghprofileId is not None:
            ghprofiledict['id'] = ghprofileId[0]
            ghrepositoryIds \
                = [id for id, in self.query(GHRepository.id) \
                   .filter(GHRepository.ghprofileId == ghprofileId[0])]
            if ghrepositoryIds:
                self.query(GHRepositorySkill) \
                    .filter(GHRepositorySkill.ghrepositoryId \
                            .in_(ghrepositoryIds)) \
                    .delete(synchronize_session=False)
                self.query(GHRepository) \
                    .filter(GHRepository.ghprofileId == ghprofileId[0]) \
                    .delete(synchronize_session=False)
        ghprofiledict = _makeGHProfile(ghprofiledict)
        repositories = ghprofiledict.pop('repositories')
        ghprofile = self.addFromDict(ghprofiledict, GHProfile)
        self.flush()

        skills = dict((skill.name, skill) for skill in ghprofile.skills)
        for repositorydict in repositories:
            tags = repositorydict.pop('tags', None)
            repositorydict['ghprofileId'] = ghprofile.id
            repository = self.addFromDict(repositorydict, GHRepository)
            self.flush()
            if tags:
                for tag in tags:
                    self.add(GHRepositorySkill(ghrepositoryId=repository.id,
                                               skillId=skills[tag].id))
                    skills[tag].score += 1.0

        return ghprofile

    def addLocation(self, nrmName, retry=False, logger=Logger(None)):
        """Add a location to the database.

        Args:
          nrmName (str): The normalized name (via ``normalizeLocation``) of the
            location.

        Returns:
          The Location object that was added to the database.

        """
        location = self.query(Location) \
                       .filter(Location.nrmName == nrmName) \
                       .first()
        if location is not None:
            if not retry or location.placeId is not None:
                return location
        else:
            location = Location(nrmName=nrmName, tries=0)
            self.add(location)

        # query Google Places API
        maxattempts = 3
        attempts = 0
        while True:
            attempts += 1
            try:
                r = requests.get(conf.PLACES_API,
                                 params={'key' : conf.PLACES_KEY,
                                         'query' : nrmName})
            except KeyboardInterrupt:
                raise
            except:
                if attempts > maxattempts:
                    logger.log('Failed request for query {0:s}\n' \
                               .format(repr(nrmName)))
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
        placeId = result['place_id']
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
        location.placeId = placeId
        location.geo = pointstr
        location.minlat = minlat
        location.minlon = minlon
        location.maxlat = maxlat
        location.maxlon = maxlon

        return location
