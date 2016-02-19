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


class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    isCompany         = Column(Boolean)
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    sector            = Column(Unicode(STR_MAX))
    nrmSector         = Column(Unicode(STR_MAX), index=True)
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    connections       = Column(Integer)
    firstExperienceStart = Column(Date)
    lastExperienceStart  = Column(Date)
    firstEducationStart  = Column(Date)
    lastEducationStart   = Column(Date)
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
    datoinId       = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('LIExperienceSkill',
                                  order_by='LIExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
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

class INProfile(SQLBase):
    __tablename__ = 'inprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    firstExperienceStart = Column(Date)
    lastExperienceStart  = Column(Date)
    firstEducationStart  = Column(Date)
    lastEducationStart   = Column(Date)
    url               = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    experiences       = relationship('INExperience',
                                     order_by='INExperience.start',
                                     cascade='all, delete-orphan')
    educations        = relationship('INEducation',
                                     order_by='INEducation.start',
                                     cascade='all, delete-orphan')
    skills            = relationship('INProfileSkill',
                                     order_by='INProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class INExperience(SQLBase):
    __tablename__ = 'inexperience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('INExperienceSkill',
                                  order_by='INExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class INEducation(SQLBase):
    __tablename__ = 'ineducation'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
    description    = Column(Unicode(STR_MAX))

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

class UWProfile(SQLBase):
    __tablename__ = 'uwprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    titlePrefix       = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    firstExperienceStart = Column(Date)
    lastExperienceStart  = Column(Date)
    firstEducationStart  = Column(Date)
    lastEducationStart   = Column(Date)
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
    skills            = relationship('UWProfileSkill',
                                     order_by='UWProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class UWExperience(SQLBase):
    __tablename__ = 'uwexperience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))

    skills         = relationship('UWExperienceSkill',
                                  order_by='UWExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class UWEducation(SQLBase):
    __tablename__ = 'uweducation'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
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
    start          = Column(Date)
    end            = Column(Date)
    description    = Column(Unicode(STR_MAX))

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

class MUProfile(SQLBase):
    __tablename__ = 'muprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    language          = Column(String(20))
    name              = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    status            = Column(Unicode(STR_MAX))
    description       = Column(Unicode(STR_MAX))
    url               = Column(String(STR_MAX))
    pictureId         = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    hqPictureUrl      = Column(String(STR_MAX))
    thumbPictureUrl   = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    skills            = relationship('MUProfileSkill',
                                     order_by='MUProfileSkill.nrmName',
                                     cascade='all, delete-orphan')
    links             = relationship('MULink',
                                     order_by='MULink.url',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

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

class MULink(SQLBase):
    __tablename__ = 'mulink'
    id            = Column(BigInteger, primary_key=True)
    muprofileId   = Column(BigInteger,
                           ForeignKey('muprofile.id'),
                           index=True)
    type          = Column(String(STR_MAX))
    url           = Column(String(STR_MAX))

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
    createdDate            = Column(DateTime)
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
    id              = Column(BigInteger, primary_key=True)
    ghprofileId     = Column(BigInteger,
                             ForeignKey('ghprofile.id'),
                             index=True)    
    name            = Column(String(STR_MAX))
    url             = Column(String(STR_MAX))
    stargazersCount = Column(Integer)
    forksCount      = Column(Integer)
    
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

def _makeLIExperience(liexperience, language, now):
    liexperience = deepcopy(liexperience)
    liexperience['language']     = language
    liexperience['parsedTitle']  = parsedTitle(language, liexperience['title'])
    liexperience['nrmTitle']     = normalizedTitle(language,
                                                   liexperience['title'])
    liexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                       liexperience['title'])
    liexperience['nrmCompany']   = normalizedCompany(language,
                                                   liexperience['company'])
    liexperience['nrmLocation']  = normalizedLocation(liexperience['location'])

    # work out duration
    duration = None        
    if liexperience['start'] is not None and liexperience['end'] is not None:
        if liexperience['start'] < liexperience['end']:
            duration = (liexperience['end'] - liexperience['start']).days
        else:
            liexperience['end'] = None
    if liexperience['start'] is None:
        liexperience['end'] = None

    return liexperience

def _makeLIEducation(lieducation, language):
    lieducation = deepcopy(lieducation)
    lieducation['language']       = language
    lieducation['nrmInstitute']   = normalizedInstitute(language,
                                                      lieducation['institute'])
    lieducation['nrmDegree']      = normalizedDegree(language,
                                                   lieducation['degree'])
    lieducation['nrmSubject']     = normalizedSubject(language,
                                                    lieducation['subject'])

    if lieducation['start'] is None:
        lieducation['end'] = None
    
    return lieducation

def _makeLIProfileSkill(skillname, language):
    nrmName = normalizedSkill(language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

def _makeLIGroup(groupname, language):
    nrmName = normalizedGroup(language, groupname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : groupname,
                'nrmName'    : nrmName}
    
def _isCompany(language, name):
    if language != 'en':
        return False
    tokens = clean(name, lowercase=True, tokenize=True)
    return ('limited' in tokens or 'ltd' in tokens)
    
def _makeLIProfile(liprofile, now):
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
    liprofile['nrmTitle']        = normalizedTitle(language, liprofile['title'])
    liprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         liprofile['title'])
    liprofile['nrmSector']       = normalizedSector(liprofile['sector'])
    liprofile['company']         = company
    liprofile['nrmCompany']      = normalizedCompany(language, company)

    # tag company profiles
    liprofile['isCompany']       = _isCompany(language, liprofile['name'])
    
    # update experiences
    liprofile['experiences'] = [_makeLIExperience(e, language, now) \
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

    return liprofile

def _makeINExperience(inexperience, language, now):
    inexperience = deepcopy(inexperience)
    inexperience['language']     = language
    inexperience['parsedTitle']  = parsedTitle(language, inexperience['title'])
    inexperience['nrmTitle']     = normalizedTitle(language,
                                                   inexperience['title'])
    inexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                       inexperience['title'])
    inexperience['nrmCompany']   = normalizedCompany(language,
                                                   inexperience['company'])
    inexperience['nrmLocation']  = normalizedLocation(inexperience['location'])

    # work out duration
    duration = None        
    if inexperience['start'] is not None and inexperience['end'] is not None:
        if inexperience['start'] < inexperience['end']:
            duration = (inexperience['end'] - inexperience['start']).days
        else:
            inexperience['end'] = None
    if inexperience['start'] is None:
        inexperience['end'] = None
        
    return inexperience

def _makeINEducation(ineducation, language):
    ineducation = deepcopy(ineducation)
    ineducation['language']       = language
    ineducation['nrmInstitute']   = normalizedInstitute(language,
                                                      ineducation['institute'])
    ineducation['nrmDegree']      = normalizedDegree(language,
                                                   ineducation['degree'])
    ineducation['nrmSubject']     = normalizedSubject(language,
                                                    ineducation['subject'])

    if ineducation['start'] is None:
        ineducation['end'] = None
    
    return ineducation

def _makeINProfileSkill(skillname, language, reenforced):
    nrmName = normalizedSkill(language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : reenforced,
                'score'      : 1.0 if reenforced else 0.0}

def _makeINProfile(inprofile, now):
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
    inprofile['nrmTitle']        = normalizedTitle(language, inprofile['title'])
    inprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         inprofile['title'])
    inprofile['company']         = company
    inprofile['nrmCompany']      = normalizedCompany(language, company)
    
    # update experiences
    inprofile['experiences'] = [_makeINExperience(e, language, now) \
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

    # add skills
    profileskills = set(inprofile['skills'])
    allskills = set(inprofile['skills'])
    for inexperience in inprofile['experiences']:
        allskills.update(inexperience['skills'])
    inprofile['skills'] = []
    for skill in allskills:
        inprofile['skills'] \
            .append(_makeINProfileSkill(skill, language,
                                        skill in profileskills))

    return inprofile


def _makeUWExperience(uwexperience, language, now):
    uwexperience = deepcopy(uwexperience)
    uwexperience['language']     = language
    uwexperience['parsedTitle']  = parsedTitle(language, uwexperience['title'])
    uwexperience['nrmTitle']     = normalizedTitle(language,
                                                   uwexperience['title'])
    uwexperience['titlePrefix']  = normalizedTitlePrefix(language,
                                                         uwexperience['title'])
    uwexperience['nrmCompany']   = normalizedCompany(language,
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
    uweducation['language']     = language
    uweducation['nrmInstitute'] = normalizedInstitute(language,
                                                      uweducation['institute'])
    uweducation['nrmDegree']    = normalizedDegree(language,
                                                   uweducation['degree'])
    uweducation['nrmSubject']   = normalizedSubject(language,
                                                    uweducation['subject'])

    if uweducation['start'] is not None and uweducation['end'] is not None:
        if uweducation['start'] >= uweducation['end']:
            uweducation['end'] = None
    if uweducation['start'] is None:
        uweducation['end'] = None
    
    return uweducation

def _makeUWProfileSkill(skillname, language):
    nrmName = normalizedSkill(language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

def _makeUWProfile(uwprofile, now):
    # get profile language
    language = uwprofile.get('language', None)

    # normalize fields
    uwprofile['nrmLocation']     = normalizedLocation(uwprofile['location'])
    uwprofile['parsedTitle']     = parsedTitle(language, uwprofile['title'])
    uwprofile['nrmTitle']        = normalizedTitle(language, uwprofile['title'])
    uwprofile['titlePrefix']     = normalizedTitlePrefix(language,
                                                         uwprofile['title'])
    
    # update experiences
    uwprofile['experiences'] = [_makeUWExperience(e, language, now) \
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

    return uwprofile

def _makeMUProfile(muprofile, now):
    # get profile language
    language = muprofile.get('language', None)

    # normalize fields
    muprofile['nrmLocation'] = normalizedLocation(muprofile['location'])

    # add skills
    muprofile['skills'] = [_makeMUProfileSkill(skill, language) \
                           for skill in muprofile['skills']]

    return muprofile

def _makeMUProfileSkill(skillname, language):
    nrmName = normalizedSkill(language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName,
                'reenforced' : False}

    
def _makeGHProfile(ghprofile, now):
    # get profile language
    language = ghprofile.get('language', None)

    # normalize fields
    ghprofile['nrmLocation'] = normalizedLocation(ghprofile['location'])
    ghprofile['nrmCompany'] = normalizedCompany(language,
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
    nrmName = normalizedSkill(language, skillname)
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
        tokenize = lambda x: splitNrmName(x)[1].split()
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

                
    def addLIProfile(self, liprofile, now):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              ``'datoinId'``
                The profile ID from DATOIN.

              ``'name'``
                The name of the LinkedIn user.

              ``'location'``
                A string describing the location of the user.

              ``'title'``
                The profile title, e.g. ``'Web developer at Geek Talent'``

              ``'description'``
                The profile summary.

              ``'profileUrl'``
                The URL of the profile.

              ``'profilePictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'crawledOn'``
                The date when the profile was crawled.

              ``'skills'``
                The skill tags listed by the user. This should be a list of 
                strings.

              ``'experiences'``
                The work experiences of the user. This should be a list of
                ``dict``s with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'title'``
                    The role/job title of the work experience.

                  ``'company'``
                    The name of the company where the person worked.

                  ``'start'``
                    The start date of the work experience.

                  ``'end'``
                    The end date of the work experience.

                  ``'description'``
                    A free-text description of the work experience.

              ``'educations'``
                The educations of the user. This should be a list of ``dict``s
                with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'institute'``
                    The name of the educational institute.

                  ``'degree'``
                    The name of the accomplished degree.

                  ``'subject'``
                    The name of the studied subject.

                  ``'start'``
                    The start date of the education.

                  ``'end'``
                    The end date of the education.

                  ``'description'``
                    A free-text description of the education.

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
        liprofile = _makeLIProfile(liprofile, now)
        liprofile = self.addFromDict(liprofile, LIProfile)
        self.flush()
        self.rankSkills(liprofile, 'linkedin')

        return liprofile

    def addINProfile(self, inprofiledict, now):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          inprofile (dict): Description of the profile. Must contain the
            following fields:

              ``'datoinId'``
                The profile ID from DATOIN.

              ``'name'``
                The name of the LinkedIn user.

              ``'location'``
                A string describing the location of the user.

              ``'title'``
                The profile title, e.g. ``'Web developer at Geek Talent'``

              ``'description'``
                The profile summary.

              ``'profileUrl'``
                The URL of the profile.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'crawledOn'``
                The date when the profile was crawled.

              ``'skills'``
                The skills mentioned in the main profile. This should be a list
                of strings.

              ``'experiences'``
                The work experiences of the user. This should be a list of
                ``dict``s with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'title'``
                    The role/job title of the work experience.

                  ``'company'``
                    The name of the company where the person worked.

                  ``'start'``
                    The start date of the work experience.

                  ``'end'``
                    The end date of the work experience.

                  ``'description'``
                    A free-text description of the work experience.

                  ``'skills'``
                    The skills mentioned in the experience record. This should
                    be a list of strings.

              ``'educations'``
                The educations of the user. This should be a list of ``dict``s
                with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'institute'``
                    The name of the educational institute.

                  ``'degree'``
                    The name of the accomplished degree.

                  ``'subject'``
                    The name of the studied subject.

                  ``'start'``
                    The start date of the education.

                  ``'end'``
                    The end date of the education.

                  ``'description'``
                    A free-text description of the education.

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
        inprofiledict = _makeINProfile(inprofiledict, now)
        inexperiences = inprofiledict.pop('experiences')
        inprofile = self.addFromDict(inprofiledict, INProfile)
        self.flush()

        # add experiences and compute skill scores
        skillIds = dict((s.name, s.id) for s in inprofile.skills)
        scores = dict((s.name, s.score) for s in inprofile.skills)
        for inexperiencedict in inexperiences:
            inexperiencedict['inprofileId'] = inprofile.id
            skills = []
            for skillname in inexperiencedict['skills']:
                skills.append({'skillId' : skillIds[skillname]})
                scores[skillname] += 1.0
            inexperiencedict['skills'] = skills
            self.addFromDict(inexperiencedict, INExperience)

        # update skill scores
        for skill in inprofile.skills:
            skill.score = scores[skill.name]

        return inprofile

    def addUWProfile(self, uwprofile, now):
        """Add a LinkedIn profile to the database (or update if it exists).

        Args:
          liprofile (dict): Description of the profile. Must contain the
            following fields:

              ``'datoinId'``
                The profile ID from DATOIN.

              ``'name'``
                The name of the LinkedIn user.

              ``'location'``
                A string describing the location of the user.

              ``'title'``
                The profile title, e.g. ``'Web developer at Geek Talent'``

              ``'description'``
                The profile summary.

              ``'profileUrl'``
                The URL of the profile.

              ``'profilePictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'crawledOn'``
                The date when the profile was crawled.

              ``'skills'``
                The skill tags listed by the user. This should be a list of 
                strings.

              ``'experiences'``
                The work experiences of the user. This should be a list of
                ``dict``s with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'title'``
                    The role/job title of the work experience.

                  ``'company'``
                    The name of the company where the person worked.

                  ``'start'``
                    The start date of the work experience.

                  ``'end'``
                    The end date of the work experience.

                  ``'description'``
                    A free-text description of the work experience.

              ``'educations'``
                The educations of the user. This should be a list of ``dict``s
                with the following fields:

                  ``'datoinId'``
                    The ID of the experience record from DATOIN.

                  ``'institute'``
                    The name of the educational institute.

                  ``'degree'``
                    The name of the accomplished degree.

                  ``'subject'``
                    The name of the studied subject.

                  ``'start'``
                    The start date of the education.

                  ``'end'``
                    The end date of the education.

                  ``'description'``
                    A free-text description of the education.

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
        uwprofile = _makeUWProfile(uwprofile, now)
        uwprofile = self.addFromDict(uwprofile, UWProfile)
        self.flush()
        self.rankSkills(uwprofile, 'upwork')

        return uwprofile

    def addMUProfile(self, muprofile, now):
        """Add a Meetup profile to the database (or update if it exists).

        Args:
          muprofile (dict): Description of the profile. Must contain the
            following fields:

              ``'datoinId'``
                The profile ID from DATOIN.

              ``'name'``
                The name of the LinkedIn user.

              ``'location'``
                A string describing the location of the user.

              ``'status'``
                The profile status

              ``'description'``
                The profile summary.

              ``'url'``
                The URL of the profile.

              ``'pictureId'``
                The ID of the profile picture.

              ``'pictureUrl'``
                The URL of the profile picture.

              ``'hqPictureUrl'``
                The URL of the high quality profile picture.

              ``'thumbPictureUrl'``
                The URL of the profile picture thumbnail.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'crawledOn'``
                The date when the profile was crawled.

              ``'skills'``
                Interests listed by the user. This should be a list of 
                strings.

        Returns:
          The MUProfile object that was added to the database.

        """
        muprofileId = self.query(MUProfile.id) \
                          .filter(MUProfile.datoinId == muprofile['datoinId']) \
                          .first()
        if muprofileId is not None:
            muprofile['id'] = muprofileId[0]
        muprofile = _makeMUProfile(muprofile, now)
        muprofile = self.addFromDict(muprofile, MUProfile)
        self.flush()
        self.rankSkills(muprofile, 'meetup')

        return muprofile
    
    def addGHProfile(self, ghprofiledict, now):
        """Add a GitHub profile to the database (or update if it exists).

        Args:
          ghprofile (dict): Description of the profile. Must contain the
            following fields:

              ``'datoinId'``
                The profile ID from DATOIN.

              ``'language'``
                The language of the profile.

              ``'name'``
                The name of the LinkedIn user.

              ``'location'``
                A string describing the location of the user.

              ``'company'``
                The user's company.

              ``'description'``
                The profile summary.

              ``'url'``
                The URL of the profile.

              ``'pictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'crawledOn'``
                The date when the profile was crawled.

              ``'skills'``
                Interests listed by the user. This should be a list of 
                strings.

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
        ghprofiledict = _makeGHProfile(ghprofiledict, now)
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
                url = r.url
                r = r.json()
            except KeyboardInterrupt:
                raise
            except:
                logger.log('Request failed. Retrying.\n')
                time.sleep(2)
                continue

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
