__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIProfileSkill',
    'LIExperienceSkill',
    'INProfile',
    'INExperience',
    'INEducation',
    'INProfileSkill',
    'INExperienceSkill',
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
from phrasematch import matchStems
from textnormalization import tokenizedSkill
import time
import random


STR_MAX = 100000

SQLBase = sqlbase()


class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
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
    indexedOn      = Column(DateTime)

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
    indexedOn      = Column(DateTime)

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

class LIExperienceSkill(SQLBase):
    __tablename__ = 'liexperience_skill'
    liexperienceId = Column(BigInteger, ForeignKey('liexperience.id'),
                            primary_key=True)
    skillId        = Column(BigInteger, ForeignKey('liprofile_skill.id'),
                            primary_key=True)
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
    liprofileId    = Column(BigInteger,
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
    indexedOn      = Column(DateTime)

    skills         = relationship('INExperienceSkill',
                                  order_by='INExperienceSkill.nrmName',
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
    indexedOn      = Column(DateTime)

class INProfileSkill(SQLBase):
    __tablename__ = 'inprofile_skill'
    id          = Column(BigInteger, primary_key=True)
    inprofileId = Column(BigInteger,
                         ForeignKey('inprofile.id'),
                         index=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)

class INExperienceSkill(SQLBase):
    __tablename__ = 'inexperience_skill'
    id          = Column(BigInteger, primary_key=True)
    inexperienceId = Column(BigInteger, ForeignKey('inexperience.id'),
                            primary_key=True)
    language    = Column(String(20))
    name        = Column(Unicode(STR_MAX))
    nrmName     = Column(Unicode(STR_MAX), index=True)

class Location(SQLBase):
    __tablename__ = 'location'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX), index=True)
    placeId   = Column(String(STR_MAX), index=True)
    geo       = Column(Geometry('POINT'))
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

    # make skills
    inexperience['skills'] = [_makeINSkill(skill, language) \
                              for skill in inexperience['skills']]
        
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

def _makeINSkill(skillname, language):
    nrmName = normalizedSkill(language, skillname)
    if not nrmName:
        return None
    else:
        return {'language'   : language,
                'name'       : skillname,
                'nrmName'    : nrmName}

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
    inprofile['skills'] = [_makeINSkill(skill, language) \
                           for skill in inprofile['skills']]

    return inprofile


class GooglePlacesError(Exception):
    pass

class CanonicalDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)        
    
    def rankSkills(self, liprofile):
        skills = liprofile.skills
        experiences = liprofile.experiences
        descriptionstems = [
            tokenizedSkill(liprofile.language,
                           _joinfields(experience.title,
                                       experience.description),
                           removebrackets=False) \
            for experience in experiences]
        skillstems = [splitNrmName(skill.nrmName)[1].split() \
                      if skill.nrmName else [] for skill in skills]

        # match experience descriptions
        matches = (matchStems(skillstems, descriptionstems,
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        for iexperience, experience in enumerate(experiences):
            experience.skills = []
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 0
                    experience.skills.append(
                        LIExperienceSkill(liexperienceId=experience.id,
                                          skillId=skill.id))

        # match profile text
        reenforced = [False]*len(skills)
        profiletext = _joinfields(liprofile.title, liprofile.description)
        profiletextstems = tokenizedSkill(liprofile.language,
                                          profiletext, removebrackets=False)
        matches = (matchStems(skillstems, [profiletextstems],
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        for iskill, skill in enumerate(skills):
            if matches[iskill, 0]:
                reenforced[iskill] = True

        # update skill ranks
        for iskill, skill in enumerate(skills):
            skill.reenforced = reenforced[iskill]

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

                  ``'indexedOn'``
                    The date when the record was indexed.

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

                  ``'indexedOn'``
                    The date when the record was indexed.

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
        self.rankSkills(liprofile)

        return liprofile

    def addINProfile(self, inprofile, now):
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

                  ``'indexedOn'``
                    The date when the record was indexed.

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

                  ``'indexedOn'``
                    The date when the record was indexed.

        Returns:
          The INProfile object that was added to the database.

        """
        inprofileId = self.query(INProfile.id) \
                          .filter(INProfile.datoinId == inprofile['datoinId']) \
                          .first()
        if inprofileId is not None:
            inprofile['id'] = inprofileId[0]
            inexperienceIds \
                = [id for id, in self.query(INExperience.id) \
                   .filter(INExperience.inprofileId == inprofileId[0])]
            if inexperienceIds:
                self.query(INExperienceSkill) \
                    .filter(INExperienceSkill.inexperienceId \
                            .in_(inexperienceIds)) \
                    .delete(synchronize_session=False)
        inprofile = _makeINProfile(inprofile, now)
        inprofile = self.addFromDict(inprofile, INProfile)
        self.flush()

        return inprofile

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

        return location
