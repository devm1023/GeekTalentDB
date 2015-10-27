__all__ = [
    'LIProfile',
    'Experience',
    'LIProfileSkill',
    'ExperienceSkill',
    'Skill',
    'Title',
    'Company',
    'Location',
    'AnalyticsDB',
    ]

import conf
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
from sqlalchemy.orm import relationship
from geoalchemy2 import Geometry


STR_MAX = 100000

SQLBase = sqlbase()

class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id                = Column(BigInteger, primary_key=True)
    datoinId          = Column(String(STR_MAX), index=True)
    name              = Column(Unicode(STR_MAX))
    placeId           = Column(String(STR_MAX), ForeignKey('location.placeId'))
    nrmTitle          = Column(Unicode(STR_MAX),
                               ForeignKey('title.nrmName'),
                               index=True)
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('company.nrmName'),
                               index=True)
    description       = Column(Unicode(STR_MAX))
    totalExperience   = Column(Integer)
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(Date, index=True)

    title = relationship('Title')
    company = relationship('Company')
    skills = relationship('LIProfileSkill',
                          cascade='all, delete-orphan')
    experiences = relationship('Experience',
                               order_by='Experience.start',
                               cascade='all, delete-orphan')
    
    __table_args__ = (UniqueConstraint('datoinId'),)

class Experience(SQLBase):
    __tablename__ = 'experience'
    id             = Column(BigInteger, primary_key=True)
    datoinId       = Column(String(STR_MAX))
    liprofileId    = Column(BigInteger,
                            ForeignKey('liprofile.id'),
                            index=True)
    nrmTitle       = Column(Unicode(STR_MAX),
                            ForeignKey('title.nrmName'),
                            index=True)
    nrmCompany     = Column(Unicode(STR_MAX),
                            ForeignKey('company.nrmName'),
                            index=True)
    start          = Column(Date)
    end            = Column(Date)
    duration       = Column(Integer)
    description    = Column(Unicode(STR_MAX))
    indexedOn      = Column(Date)

    title = relationship('Title')
    company = relationship('Company')
    skills = relationship('ExperienceSkill')

    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         primary_key=True)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('skill.nrmName'),
                         primary_key=True)
    rank        = Column(Float)

    skill = relationship('Skill')

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experienceId = Column(BigInteger,
                          ForeignKey('experience.id'),
                          primary_key=True)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('skill.nrmName'),
                          primary_key=True)
    
    skill = relationship('Skill')
    
class Skill(SQLBase):
    __tablename__ = 'skill'
    nrmName         = Column(Unicode(STR_MAX), primary_key=True)
    name            = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class Title(SQLBase):
    __tablename__ = 'title'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class Company(SQLBase):
    __tablename__ = 'company'
    nrmName   = Column(Unicode(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX))
    liprofileCount  = Column(BigInteger)
    experienceCount = Column(BigInteger)

class Location(SQLBase):
    __tablename__ = 'location'
    placeId   = Column(String(STR_MAX), primary_key=True)
    name      = Column(Unicode(STR_MAX))
    geo       = Column(Geometry('POINT'))


    
class AnalyticsDB(SQLDatabase):
    def __init__(self, url=None, session=None, engine=None):
        SQLDatabase.__init__(self, SQLBase.metadata,
                             url=url, session=session, engine=engine)

    def addSkill(self, nrmName, name, liprofileCount, experienceCount):
        skill = self.query(Skill).filter(Skill.nrmName == nrmName) \
                                 .first()
        if not skill:
            skill = Skill(nrmName=nrmName)
            self.add(skill)
        skill.name = name
        skill.liprofileCount = liprofileCount
        skill.experienceCount = experienceCount
        return skill

    def addTitle(self, nrmName, name, liprofileCount, experienceCount):
        title = self.query(Title).filter(Title.nrmName == nrmName) \
                                 .first()
        if not title:
            title = Title(nrmName=nrmName)
            self.add(title)
        title.name = name
        title.liprofileCount = liprofileCount
        title.experienceCount = experienceCount
        return title

    def addCompany(self, nrmName, name, liprofileCount, experienceCount):
        company = self.query(Company).filter(Company.nrmName == nrmName) \
                                     .first()
        if not company:
            company = Company(nrmName=nrmName)
            self.add(company)
        company.name = name
        company.liprofileCount = liprofileCount
        company.experienceCount = experienceCount
        return company

    def addLocation(self, placeId, name, geo):
        if placeId is None:
            return Location()
        location = self.query(Location).filter(Location.placeId == placeId) \
                                       .first()
        if not location:
            location = Location(placeId=placeId)
            self.add(location)
        location.name = name
        location.geo = geo
        return location

    # def addExperience(self, experiencedict):
    #     experience = self.query(Experience) \
    #                      .filter(Experience.id == experiencedict['id']) \
    #                      .first()
    #     if experience:
    #         updateRowFromDict(experience, experiencedict)
    #     else:
    #         experience = rowFromDict(experiencedict, Experience)
    #         self.add(experience)

    #     skills = experiencedict.get('skills', [])
    #     q = self.query(ExperienceSkill) \
    #             .filter(ExperienceSkill.experienceId == experience.id)
    #     if skills:
    #         q = q.filter(~ExperienceSkill.nrmSkill.isin(skills))
    #     q.delete()
    #     for nrmSkill in skills:
    #         experienceskill = self.query(ExperienceSkill) \
    #                               .filter(ExperienceSkill.experienceId \
    #                                       == experience.id) \
    #                               .filter(ExperienceSkill.nrmSkill == nrmSkill) \
    #                               .first()
    #         if not experienceskill:
    #             self.add(ExperienceSkill(experienceId=experience.id,
    #                                      nrmSkill=nrmSkill))
    #     return experience
            
    
    # def addLIProfile(self, liprofiledict):
    #     liprofile = self.query(LIProfile) \
    #                     .filter(LIProfile.id == liprofiledict['id'])
    #     if liprofile:
    #         updateRowFromDict(liprofile, liprofiledict)
    #     else:
    #         liprofile = rowFromDict(liprofile, LIProfile)
    #         self.add(liprofile)

    #     experienceIds = [e['id'] for e in liprofiledict['experiences']]
    #     q = self.query(Experience) \
    #             .filter(Experience.liprofileId == liprofile.id)
    #     if experienceIds:
    #         q = q.filter(~Experience.id.isin(experienceIds))
    #     q.delete()
    #     for experience in liprofiledict['experiences']:
    #         self.addExperience(experience)

    #     profileskills = [s['nrmName'] for s in liprofiledict['skills']]
        
