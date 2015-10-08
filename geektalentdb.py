__all__ = [
    'LIProfile',
    'Skill',
    'Role',
    'RoleName',
    'Location',
    'LocationName',
    'Experience',
    'ExperienceSkill',
    'LIProfileSkill',
    'GeekTalentDB',
    ]

import conf
from phrasematch import clean, stem, tokenize, matchStems
from datetime import datetime
import re
import numpy as np

from sqldb import *
from sqlalchemy import \
    Column, \
    UniqueConstraint, \
    ForeignKey, \
    Integer, \
    BigInteger, \
    Unicode, \
    UnicodeText, \
    String, \
    Text, \
    Date, \
    Float, \
    func
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry

import requests


STR_MAX = 100000
DATE0 = datetime(year=1800, month=1, day=1)


SQLBase = sqlbase()

# entities

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id = Column(BigInteger, primary_key=True)
    parentId = Column(String(STR_MAX))
    name = Column(Unicode(STR_MAX))
    title = Column(Unicode(STR_MAX))
    description = Column(Unicode(STR_MAX))
    company = Column(Unicode(STR_MAX))
    url = Column(String(STR_MAX))
    pictureUrl = Column(String(STR_MAX))

    locationId = Column(BigInteger, ForeignKey('location.id'))
    roleId = Column(BigInteger, ForeignKey('role.id'))
    companyId = Column(BigInteger, ForeignKey('company.id'))

    skills = relationship('LIProfileSkill')
    experiences = relationship('Experience', order_by='Experience.startdate')

class Location(SQLBase):
    __tablename__ = 'location'
    id = Column(BigInteger, primary_key=True)
    name = Column(Unicode(STR_MAX))
    placeId = Column(String(STR_MAX))
    geo = Column(Geometry('POINT'))

class LocationName(SQLBase):
    __tablename__ = 'location_name'
    nname = Column(Unicode(STR_MAX), primary_key=True)
    locationId = Column(BigInteger, ForeignKey('location.id'))

class Skill(SQLBase):
    __tablename__ = 'skill'
    id = Column(BigInteger, primary_key=True)
    nname = Column(Unicode(STR_MAX))

class SkillName(SQLBase):
    __tablename__ = 'skill_name'
    skillId = Column(BigInteger,
                     ForeignKey('skill.id'),
                     primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(Unicode(STR_MAX))
    count = Column(Integer)

class Role(SQLBase):
    __tablename__ = 'role'
    id = Column(BigInteger, primary_key=True)
    nname = Column(Unicode(STR_MAX))

class RoleName(SQLBase):
    __tablename__ = 'role_name'
    roleId = Column(BigInteger,
                    ForeignKey('role.id'),
                    primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(Unicode(STR_MAX))
    count = Column(Integer)

class Company(SQLBase):
    __tablename__ = 'company'
    id = Column(BigInteger, primary_key=True)
    nname = Column(Unicode(STR_MAX))

class CompanyName(SQLBase):
    __tablename__ = 'company_name'
    companyId = Column(BigInteger,
                       ForeignKey('company.id'),
                       primary_key=True)
    id = Column(BigInteger,
                primary_key=True)
    name = Column(Unicode(STR_MAX))
    count = Column(Integer)

class Experience(SQLBase):
    __tablename__ = 'experience'
    id = Column(BigInteger, primary_key=True)
    startdate = Column(Date)
    enddate = Column(Date)
    duration = Column(Float)
    title = Column(Unicode(STR_MAX))
    description = Column(Unicode(STR_MAX))
    companyName = Column(Unicode(STR_MAX))
 
    liprofileId = Column(BigInteger, ForeignKey('liprofile.id'))
    companyId = Column(BigInteger, ForeignKey('company.id'))
    roleId = Column(BigInteger, ForeignKey('role.id'))
    
    skills = relationship('ExperienceSkill')


# associations

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experienceId = Column(BigInteger,
                          ForeignKey('experience.id'),
                          primary_key=True)
    skillId = Column(BigInteger,
                     ForeignKey('skill.id'),
                     primary_key=True)

class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         primary_key=True,
                         index=True)
    skillId = Column(BigInteger,
                     ForeignKey('skill.id'),
                     primary_key=True,
                     index=True)
    rank = Column(Float)


# string normalisations

def normalizedSkill(name):
    if not name:
        return None
    nname = stem(name)
    if not nname:
        return None
    nname.sort()
    return ' '.join(nname)

def normalizedRole(name):
    if not name:
        return None
    name = clean(name, keep='&/-', removebrackets=True)
    name = name.split(' - ')[0]
    name = name.split(' at ')[0]
    name = name.split(',')[0]
    nname = stem(name, removebrackets=True)
    if not nname:
        return None
    return ' '.join(nname)

def normalizedCompany(name):
    if not name:
        return None
    nname = tokenize(name, removebrackets=True)
    if not nname:
        return None
    return ' '.join(nname)

def _getStartDate(experience):
    startdate = experience.get('startdate', DATE0)
    if startdate is None:
        return DATE0
    return startdate


class GeekTalentDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addSkill(self, name):
        nname = normalizedSkill(name)
        if not nname:
            return Skill()

        skill = self.query(Skill).filter(Skill.nname == nname).first()
        if not skill:
            skill = Skill(nname=nname)
            self.add(skill)
            self.flush()

        skillname = self.query(SkillName) \
                        .filter(SkillName.skillId == skill.id) \
                        .filter(SkillName.name == name) \
                        .first()
        if not skillname:
            skillname = SkillName(skillId=skill.id, name=name, count=1)
            self.add(skillname)
        else:
            skillname.count += 1
        self.flush()
        
        return skill

    def addRole(self, name):
        nname = normalizedRole(name)
        if not nname:
            return Role()

        role = self.query(Role) \
                       .filter(Role.nname == nname).first()
        if not role:
            role = Role(nname=nname)
            self.add(role)
            self.flush()

        rolename = self.query(RoleName) \
                           .filter(RoleName.roleId == role.id) \
                           .filter(RoleName.name == name) \
                           .first()
        if not rolename:
            rolename = RoleName(roleId=role.id,
                                        name=name, count=1)
            self.add(rolename)
        else:
            rolename.count += 1
        self.flush()
            
        return role

    def addCompany(self, name):
        nname = normalizedCompany(name)
        if not nname:
            return Company()

        company = self.query(Company) \
                      .filter(Company.nname == nname).first()
        if not company:
            company = Company(nname=nname)
            self.add(company)
            self.flush()

        companyname = self.query(CompanyName) \
                          .filter(CompanyName.companyId == company.id) \
                          .filter(CompanyName.name == name) \
                          .first()
        if not companyname:
            companyname = CompanyName(companyId=company.id,
                                      name=name, count=1)
            self.add(companyname)
        else:
            companyname.count += 1
        self.flush()
            
        return company

    def addLocation(self, name):
        nname = ' '.join(name.lower().split())
        locationname = self.query(LocationName) \
                           .filter(LocationName.nname == nname).first()
        if not locationname:
            r = requests.get(conf.PLACES_API,
                             params={'key' : conf.PLACES_KEY,
                                     'query' : nname}).json()
            if len(r['results']) != 1:
                return Location()
            lat = r['results'][0]['geometry']['location']['lat']
            lon = r['results'][0]['geometry']['location']['lng']
            address = r['results'][0]['formatted_address']
            placeId = r['results'][0]['place_id']

            # format address
            address = address.split(', ')
            if address:
                for i, s in enumerate(address[:-1]):
                    while len(address) > i+1 and address[i+1] == s:
                        del address[i+1]
            address = ', '.join(address)

            # find location
            location = self.query(Location) \
                           .filter(Location.placeId == placeId) \
                           .first()
            if not location:
                pointstr = 'POINT({0:f} {1:f})'.format(lon, lat)
                location = Location(geo=pointstr,
                                    name=address,
                                    placeId=placeId)
                self.add(location)
                self.flush()
                
            locationname = LocationName(nname=nname, locationId=location.id)
            self.add(locationname)
            self.flush()
        else:
            location = self.query(Location) \
                           .filter(Location.id == locationname.locationId) \
                           .one()

        return location

    
    def deleteExperienceSkills(self, experienceIds):
        if hasattr(experienceIds, '__len__'):
            ids = tuple(experienceIds)
        else:
            ids = (experienceIds,)
        if ids:
            self.query(ExperienceSkill) \
                .filter(ExperienceSkill.experienceId.in_(ids)) \
                .delete(synchronize_session='fetch')
            self.flush()

    def addExperience(self, liprofileId, companyname, startdate, enddate,
                      title, description):
        # determine duration of work experience
        duration = None
        if startdate is not None and enddate is not None:
            if startdate < enddate:
                duration = (enddate - startdate).days

        # add company
        company = self.addCompany(companyname)

        # add role
        role = self.addRole(title)

        # add experience record
        experience = Experience(liprofileId=liprofileId,
                                companyId=company.id,
                                startdate=startdate,
                                enddate=enddate,
                                duration=duration,
                                title=title,
                                description=description,
                                roleId=role.id)
        self.add(experience)
        self.flush()

        return experience

    def addLIProfileSkills(self, liprofile, skills):
        liprofileskills = []
        skillids = set()
        for skillname in skills:
            skill = self.addSkill(skillname)
            if skill.id and skill.id not in skillids:
                skillids.add(skill.id)
                liprofileskills.append(
                    LIProfileSkill(liprofileId=liprofile.id,
                                   skillId=skill.id,
                                   rank=0.0))
        liprofile.skills = liprofileskills
        self.flush()
        return liprofileskills

    def rankSkills(self, liprofileskills, experiences):
        skills = [
            self.query(Skill).filter(Skill.id == ps.skillId) \
                             .one() for ps in liprofileskills]
        descriptions = [' '.join([e.title if e.title else '',
                                  e.description if e.description else '']) \
                        for e in experiences]
        descriptionstems = list(map(stem, descriptions))
        skillstems = [s.nname.split() for s in skills]
        matches = (matchStems(skillstems, descriptionstems,
                              threshold=conf.SKILL_MATCHING_THRESHOLD) > \
                   conf.SKILL_MATCHING_THRESHOLD)
        ranks = np.zeros(len(skills))
        for iexperience, experience in enumerate(experiences):
            experienceskills = []
            for iskill, skill in enumerate(skills):
                if matches[iskill, iexperience]:
                    if experience.duration:
                        duration = experience.duration
                    else:
                        duration = 365
                    ranks[iskill] += duration/365.0
                    experienceskills.append(
                        ExperienceSkill(experienceId=experience.id,
                                        skillId=skill.id))
            experience.skills = experienceskills
        for liprofileskill, rank in zip(liprofileskills, ranks):
            liprofileskill.rank = rank

        self.flush()
        return liprofileskills

    def addLIProfile(self, parentId, name, title, description,
                     locationname, url, pictureUrl,
                     skills, experiencedicts):
        # sort experiences
        sorted_experiences = sorted(experiencedicts, key=_getStartDate)

        # add location
        location = self.addLocation(locationname)

        # add role
        role = self.addRole(title)

        # add company
        if sorted_experiences:
            companyname = sorted_experiences[-1]['company']
            company = self.addCompany(companyname)
        else:
            companyname = None
            company = Company()
            

        # add profile
        liprofile = LIProfile(parentId=parentId,
                              name=name,
                              title=title,
                              description=description,
                              company=companyname,
                              url=url,
                              pictureUrl=pictureUrl,
                              locationId=location.id,
                              roleId=role.id,
                              companyId=company.id)
        self.add(liprofile)
        self.flush()

        # add skills
        liprofileskills = self.addLIProfileSkills(liprofile, skills)

        # add experiences
        experiences = [self.addExperience(liprofile.id,
                                          experience['company'],
                                          experience['startdate'],
                                          experience['enddate'],
                                          experience['title'],
                                          experience['description']) \
                       for experience in experiencedicts]

        # compute skill ranks
        self.rankSkills(liprofileskills, experiences)
        
        return liprofile

