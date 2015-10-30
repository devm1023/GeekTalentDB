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
                               nullable=True,
                               index=True)
    nrmCompany        = Column(Unicode(STR_MAX),
                               ForeignKey('company.nrmName'),
                               nullable=True,
                               index=True)
    description       = Column(Unicode(STR_MAX))
    totalExperience   = Column(Integer)
    profileUrl        = Column(String(STR_MAX))
    profilePictureUrl = Column(String(STR_MAX))
    indexedOn         = Column(Date, index=True)

    title = relationship('Title')
    company = relationship('Company')
    skills = relationship('LIProfileSkill',
                          order_by='LIProfileSkill.nrmName',
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
    skills = relationship('ExperienceSkill',
                          order_by='ExperienceSkill.nrmSkill',
                          cascade='all, delete-orphan')

    
class LIProfileSkill(SQLBase):
    __tablename__ = 'liprofile_skill'
    liprofileId = Column(BigInteger,
                         ForeignKey('liprofile.id'),
                         primary_key=True,
                         autoincrement=False)
    nrmName     = Column(Unicode(STR_MAX),
                         ForeignKey('skill.nrmName'),
                         primary_key=True,
                         autoincrement=False)
    rank        = Column(Float)

    skill = relationship('Skill')

class ExperienceSkill(SQLBase):
    __tablename__ = 'experience_skill'
    experienceId = Column(BigInteger,
                          ForeignKey('experience.id'),
                          primary_key=True,
                          autoincrement=False)
    nrmSkill     = Column(Unicode(STR_MAX),
                          ForeignKey('skill.nrmName'),
                          primary_key=True,
                          autoincrement=False)
    
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

    def addLIProfile(self, liprofile):
        """Add a LinkedIn profile to the database.

        Args:
          liprofile (dict): A ``dict`` describing the LinkedIn profile. It must
            contain the following fields:

              ``'datoinId'``
                The ID of the profile from DATOIN.

              ``'name'``
                The name of the LinkedIn user.

              ``'placeId'``
                The Google Place ID for the user's location.

              ``'nrmTitle'``
                The normalized profile title.

              ``'nrmCompany'``
                The normalized name of the user's current company.

              ``'description'``
                The profile summary.

              ``'totalExperience'``
                The total work experience in days.

              ``'profileUrl'``
                The URL of the profile.

              ``'profilePictureUrl'``
                The URL of the profile picture.

              ``'indexedOn'``
                The date when the profile was indexed.

              ``'skills'``
                The skills declared by the user. This should be a list of 
                ``dict``s with the following fields:

                  ``'nrmName'``
                    The normalized name of the skill.

                  ``'rank'``
                    The rank of the skill.

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
                    The skills that are explicitely mentioned in this experience
                    record. This should be a list of normalized skill names.
              
        Returns:
          The ``LIProfile`` object that was added to the database.

        """

        liprofile.pop('title', None)
        liprofile.pop('company', None)
        
        if liprofile.get('skills', None) is not None:
            skillnames = set()
            newskills = []
            for skill in liprofile['skills']:
                skill.pop('liprofileId', None)
                skill.pop('skill', None)
                if skill['nrmName'] not in skillnames:
                    skillnames.add(skill['nrmName'])
                    newskills.append(skill)
            liprofile['skills'] = newskills

        if liprofile.get('experiences', None) is not None:
            for experience in liprofile['experiences']:
                experience.pop('id', None)
                experience.pop('liprofileId', None)
                experience.pop('title', None)
                experience.pop('company', None)
                if experience.get('skills', None) is not None:
                    skillnames = set()
                    newskills = []
                    for skill in experience['skills']:
                        if skill not in skillnames:
                            newskills.append({'nrmSkill' : skill})
                            skillnames.add(skill)
                    experience['skills'] = newskills

        return self.addFromDict(liprofile, LIProfile)

