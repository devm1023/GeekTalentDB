__all__ = [
    'LIProfile',
    'Experience',
    'Education',
    'NormalFormDB',
    ]

import conf
import numpy as np
import requests
from sqldb import *
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
    Date, \
    Float, \
    func
from geoalchemy2 import Geometry
from phrasematch import clean, stem, tokenize, matchStems


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
    company           = Column(Unicode(STR_MAX))
    nrmCompany        = Column(Unicode(STR_MAX), index=True)
    description       = Column(Unicode(STR_MAX))
    totalExperience   = Column(Integer)
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(Date, index=True)

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
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(Date)

class Education(SQLBase):
    __tablename__ = 'education'
    id          = Column(Integer, primary_key=True)
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
    indexedOn      = Column(Date)

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

class Location(SQLBase):
    __tablename__ = 'location'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX))
    placeId   = Column(String(STR_MAX))
    geo       = Column(Geometry('POINT'))


def normalizedSkill(name):
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    nname.sort()
    return ' '.join(nname)

def parsedTitle(name):
    if not name:
        return None
    name = clean(name, keep='&/-,\'', removebrackets=True)
    name = name.split(' - ')[0]
    name = name.split(' / ')[0]
    name = name.split(' at ')[0]
    name = name.split(' for ')[0]
    name = name.split(',')[0]
    return name
    
def normalizedTitle(name):
    name = parsedTitle(name)
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    return ' '.join(nname)

def normalizedCompany(name):
    if not name:
        return None
    nname = clean(name, keep=',-/&', nospace='\'', removebrackets=True).lower()
    nname = nname.split(',')[0]
    nname = nname.split(' - ')[0]
    nname = nname.split(' / ')[0]
    nname = nname.split(' & ')[0]
    nname = nname.replace(' limited', ' ltd')
    nname = clean(nname)
    if not nname:
        return None
    return nname

def normalizedLocation(name):
    return ' '.join(name.lower().split())

def _joinfields(*args):
    return ' '.join([a for a in args if a])

    
class NormalFormDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addExperience(self, profileId, edict, now):        
        experience = Experience()
        experience.datoinId       = edict['datoinId']
        experience.profileId      = profileId
        experience.title          = edict['title']
        experience.parsedTitle    = parsedTitle(edict['title'])
        experience.nrmTitle       = normalizedTitle(edict['title'])
        experience.company        = edict['company']
        experience.nrmCompany     = normalizedCompany(edict['company'])
        experience.start          = edict['start']
        experience.end            = edict['end']
        experience.description    = edict['description']
        experience.indexedOn      = edict['indexedOn']

        # work out duration
        duration = None        
        if experience.start is not None and experience.end is not None:
            if experience.start < experience.end:
                duration = (experience.end - experience.start).days
        elif experience.start is not None:
            duration = (now - experience.start).days
        experience.duration = duration
        
        self.add(experience)
        return experience

    def addEducation(self, profileId, edict):
        education = Education()
        education.datoinId       = edict['datoinId']
        education.profileId      = profileId
        education.institute      = edict['institute']
        education.nrmInstitute   = normalizedInstitute(edict['institute'])
        education.degree         = edict['degree']
        education.nrmDegree      = normalizedDegree(edict['degree'])
        education.subject        = edict['subject']
        education.nrmSubject     = normalizedSubject(edict['subject'])
        education.start          = edict['start']
        education.end            = edict['end']
        education.description    = edict['description']
        education.indexedOn      = edict['indexedOn']
        self.add(education)
        return education

    def addSkill(self, profileId, skillname):
        skill = Skill()
        skill.profileId = profileId
        skill.name      = skillname
        skill.nrmName   = normalizedSkill(skillname)
        skill.rank      = 0.0
        self.add(skill)
        return skill

    def rankSkills(self, skills, experiences, liprofile):
        descriptionstems = [stem(_joinfields(experience.title,
                                             experience.description)) \
                            for experience in experiences]
        skillstems = [skill.nrmName.split() if skill.nrmName else [] \
                      for skill in skills]

        # match experience descriptions
        matches = (matchStems(skillstems, descriptionstems,
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        ranks = np.zeros(len(skills))
        for iexperience, experience in enumerate(experiences):
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 0
                    ranks[iskill] += duration/365.0
                    self.add(ExperienceSkill(experienceId=experience.id,
                                             skillId=skill.id))

        # match profile text
        profiletext = _joinfields(liprofile.title, liprofile.description)
        matches = (matchStems(skillstems, [stem(profiletext)],
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
        
    
    def addLIProfile(self, profile, experiencedicts, educationdicts, now):
        # determine current company
        company = None
        currentexperiences = [e for e in experiencedicts \
                              if e['start'] is not None and e['end'] is None]
        if len(currentexperiences) == 1:
            company = currentexperiences[0]['company']            

        # create or update LIProfile
        liprofile = self.query(LIProfile) \
                        .filter(LIProfile.datoinId == profile['datoinId']) \
                        .first()
        if not liprofile:
            isnew = True
            liprofile = LIProfile()
        else:
            isnew = False

        liprofile.datoinId        = profile['datoinId']
        liprofile.name            = profile['name']
        liprofile.nrmLocation     = normalizedLocation(profile['location'])
        liprofile.title           = profile['title']
        liprofile.parsedTitle     = parsedTitle(profile['title'])
        liprofile.nrmTitle        = normalizedTitle(profile['title'])
        liprofile.company         = company
        liprofile.nrmCompany      = normalizedCompany(company)
        liprofile.description     = profile['description']
        liprofile.totalexperience = 0
        liprofile.url             = profile['url']
        liprofile.pictureUrl      = profile['pictureUrl']
        liprofile.indexedOn       = profile['indexedOn']

        if isnew:
            self.add(liprofile)
            self.flush()

        # add experiences
        if not isnew:
            for experience in self.query(Experience) \
                                  .filter(Experience.profileId == liprofile.id) :
                self.query(ExperienceSkill) \
                    .filter(ExperienceSkill.experienceId == experience.id) \
                    .delete(synchronize_session='fetch')
                self.session.delete(experience)
        experiences = [self.addExperience(liprofile.id, e, now) \
                       for e in experiencedicts]
        liprofile.totalExperience = sum([e.duration for e in experiences \
                                         if e.duration is not None])

        # add educations
        if not isnew:
            self.query(Education) \
                .filter(Education.profileId == liprofile.id) \
                .delete(synchronize_session='fetch')
        for edict in educationdicts:
            self.addEducation(liprofile.id, edict)

        # add skills
        if not isnew:
            self.query(Skill) \
                .filter(Skill.profileId == liprofile.id) \
                .delete(synchronize_session='fetch')
        skills = [self.addSkill(liprofile.id, skill) \
                  for skill in profile['skills']]

        # flush session
        self.flush()

        # rank skills and fill ExperienceSkill
        self.rankSkills(skills, experiences, liprofile)
        
        return liprofile

    def addLocation(self, nrmName):
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
