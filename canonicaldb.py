__all__ = [
    'LIProfile',
    'Experience',
    'Education',
    'Skill',
    'ExperienceSkill',
    'Location',
    'CanonicalDB',
    ]

import conf
import numpy as np
import requests
from copy import deepcopy
from sqldb import *
from textnormalization import *
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
    func
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry
from phrasematch import matchStems
from textnormalization import tokenizedSkill


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    name              = Column(Unicode(STR_MAX))
    location          = Column(Unicode(STR_MAX))
    nrmLocation       = Column(Unicode(STR_MAX), index=True)
    title             = Column(Unicode(STR_MAX))
    parsedTitle       = Column(Unicode(STR_MAX))
    nrmTitle          = Column(Unicode(STR_MAX), index=True)
    sector            = Column(Unicode(STR_MAX))
    nrmSector         = Column(Unicode(STR_MAX), index=True)
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    connections       = Column(Integer)
    totalExperience   = Column(Integer) # total work experience in days
    url               = Column(String(STR_MAX))
    pictureUrl        = Column(String(STR_MAX))
    indexedOn         = Column(DateTime, index=True)
    crawledOn         = Column(DateTime, index=True)

    experiences       = relationship('Experience',
                                     order_by='Experience.start',
                                     cascade='all, delete-orphan')
    educations        = relationship('Education',
                                     order_by='Education.start',
                                     cascade='all, delete-orphan')
    skills            = relationship('Skill',
                                     order_by='Skill.nrmName',
                                     cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class Experience(SQLBase):
    __tablename__ = 'experience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    profileId      = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
    title          = Column(Unicode(STR_MAX))
    parsedTitle    = Column(Unicode(STR_MAX))
    nrmTitle       = Column(Unicode(STR_MAX), index=True)
    company        = Column(Unicode(STR_MAX))
    nrmCompany     = Column(Unicode(STR_MAX), index=True)
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer) # duration in days
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(DateTime)

    skills         = relationship('ExperienceSkill',
                                  order_by='ExperienceSkill.skillId',
                                  cascade='all, delete-orphan')

class Education(SQLBase):
    __tablename__ = 'education'
    id          = Column(BigInteger, primary_key=True)
    datoinId    = Column(String(STR_MAX))
    profileId   = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         index=True)
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

class Skill(SQLBase):
    __tablename__ = 'skill'
    id        = Column(BigInteger, primary_key=True)
    profileId = Column(BigInteger,
                       ForeignKey('liprofile.id'),
                       index=True)
    name      = Column(Unicode(STR_MAX))
    nrmName   = Column(Unicode(STR_MAX), index=True)
    rank      = Column(Float)

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experienceId = Column(BigInteger, ForeignKey('experience.id'),
                          primary_key=True)
    skillId      = Column(BigInteger, ForeignKey('skill.id'),
                          primary_key=True)
    skill        = relationship('Skill')

class Location(SQLBase):
    __tablename__ = 'location'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX), index=True)
    placeId   = Column(String(STR_MAX), index=True)
    geo       = Column(Geometry('POINT'))


def _joinfields(*args):
    return ' '.join([a for a in args if a])

def _makeExperience(experience, now):
    experience = deepcopy(experience)
    experience['parsedTitle']  = parsedTitle(experience['title'])
    experience['nrmTitle']     = normalizedTitle(experience['title'])
    experience['nrmCompany']   = normalizedCompany(experience['company'])

    # work out duration
    duration = None        
    if experience['start'] is not None and experience['end'] is not None:
        if experience['start'] < experience['end']:
            duration = (experience['end'] - experience['start']).days
    elif experience['start'] is not None:
        duration = (now - experience['start']).days
    experience['duration'] = duration

    return experience

def _makeEducation(education):
    education = deepcopy(education)
    education['nrmInstitute']   = normalizedInstitute(education['institute'])
    education['nrmDegree']      = normalizedDegree(education['degree'])
    education['nrmSubject']     = normalizedSubject(education['subject'])
    return education

def _makeSkill(skillname):
    nrmName = normalizedSkill(skillname)
    if not nrmName:
        return None
    else:
        return {'name' : skillname, 'nrmName' : nrmName, 'rank' : 0.0}

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

    liprofile['nrmLocation']     = normalizedLocation(liprofile['location'])
    liprofile['parsedTitle']     = parsedTitle(liprofile['title'])
    liprofile['nrmTitle']        = normalizedTitle(liprofile['title'])
    liprofile['nrmSector']       = normalizedSector(liprofile['sector'])
    liprofile['company']         = company
    liprofile['nrmCompany']      = normalizedCompany(company)
    liprofile['totalExperience'] = 0

    # update experiences
    liprofile['experiences'] \
        = [_makeExperience(e, now) for e in liprofile['experiences']]
    liprofile['totalExperience'] \
        = sum([e['duration'] for e in liprofile['experiences'] \
               if e['duration'] is not None])

    # update educations
    liprofile['educations'] \
        = [_makeEducation(e) for e in liprofile['educations']]

    # add skills
    liprofile['skills'] = [_makeSkill(skill) for skill in liprofile['skills']]

    return liprofile


class CanonicalDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)        
    
    def rankSkills(self, liprofile):
        skills = liprofile.skills
        experiences = liprofile.experiences
        descriptionstems = [
            tokenizedSkill(
                _joinfields(experience.title, experience.description),
                removebrackets=False) \
            for experience in experiences]
        skillstems = [skill.nrmName.split() if skill.nrmName else [] \
                      for skill in skills]

        # match experience descriptions
        matches = (matchStems(skillstems, descriptionstems,
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        ranks = np.zeros(len(skills))
        for iexperience, experience in enumerate(experiences):
            experience.skills = []
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 0
                    ranks[iskill] += duration/365.0
                    experience.skills.append(
                        ExperienceSkill(experienceId=experience.id,
                                        skillId=skill.id))

        # match profile text
        profiletext = _joinfields(liprofile.title, liprofile.description)
        profiletextstems = tokenizedSkill(profiletext, removebrackets=False)
        matches = (matchStems(skillstems, [profiletextstems],
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        for iskill, skill in enumerate(skills):
            if matches[iskill, 0]:
                # half of the total work experience for skills mentioned in
                # the profile summary
                ranks[iskill] += liprofile.totalExperience/365.0/2.0

        # update skill ranks
        for iskill, skill in enumerate(skills):
            skill.rank = ranks[iskill]

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
            liprofile['id'] = liprofileId
        liprofile = _makeLIProfile(liprofile, now)
        liprofile = self.addFromDict(liprofile, LIProfile)
        self.flush()
        self.rankSkills(liprofile)

        return liprofile

    def addLocation(self, nrmName):
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
            return location

        # query Google Places API
        r = requests.get(conf.PLACES_API,
                         params={'key' : conf.PLACES_KEY,
                                 'query' : nrmName}).json()
        if len(r['results']) != 1:
            location = Location(nrmName=nrmName)
            self.add(location)
            return location

        # parse result
        lat = r['results'][0]['geometry']['location']['lat']
        lon = r['results'][0]['geometry']['location']['lng']
        pointstr = 'POINT({0:f} {1:f})'.format(lon, lat)
        address = r['results'][0]['formatted_address']
        placeId = r['results'][0]['place_id']

        # format address
        address = address.split(', ')
        if address:
            for i, s in enumerate(address[:-1]):
                while len(address) > i+1 and address[i+1] == s:
                    del address[i+1]
        address = ', '.join(address)

        # add record
        location = Location(nrmName=nrmName,
                            name=address,
                            placeId=placeId,
                            geo=pointstr)
        self.add(location)

        return location
