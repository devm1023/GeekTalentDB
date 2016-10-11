__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'LISkill',
    'WUSubject',
    'WUALevel',
    'WUCareer',
    'ParseDB',
    ]

import conf
from dbtools import *
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
    DateTime, \
    Float, \
    Boolean, \
    func
from sqlalchemy.orm import relationship

# character limit for string columns
STR_MAX = 100000

# base class for table objects
SQLBase = declarative_base()


class LIProfile(SQLBase):
    __tablename__ = 'liprofile'
    id            = Column(BigInteger, primary_key=True)
    timestamp     = Column(DateTime, index=True, nullable=False)
    url           = Column(String(STR_MAX), nullable=False)
    picture_url   = Column(String(STR_MAX))
    name          = Column(Unicode(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    sector        = Column(Unicode(STR_MAX))
    title         = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))
    connections   = Column(String(STR_MAX))

    experiences   = relationship('LIExperience',
                                 cascade='all, delete-orphan')
    educations    = relationship('LIEducation',
                                 cascade='all, delete-orphan')
    groups        = relationship('LIGroup',
                                 cascade='all, delete-orphan')
    skills        = relationship('LISkill',
                                 cascade='all, delete-orphan')

    __table_args__ = (UniqueConstraint('url'),)


class LIExperience(SQLBase):
    __tablename__ = 'liexperience'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    title         = Column(Unicode(STR_MAX))
    company       = Column(Unicode(STR_MAX))
    company_url   = Column(String(STR_MAX))
    logo_company  = Column(Unicode(STR_MAX))
    logo_url      = Column(String(STR_MAX))
    location      = Column(Unicode(STR_MAX))
    current       = Column(Boolean)
    start         = Column(Unicode(STR_MAX))
    end           = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))


class LIEducation(SQLBase):
    __tablename__ = 'lieducation'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    institute     = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))
    course        = Column(Unicode(STR_MAX))
    start         = Column(Unicode(STR_MAX))
    end           = Column(Unicode(STR_MAX))
    description   = Column(Unicode(STR_MAX))


class LIGroup(SQLBase):
    __tablename__ = 'ligroup'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))


class LISkill(SQLBase):
    __tablename__ = 'liskill'
    id            = Column(BigInteger, primary_key=True)
    parent_id     = Column(BigInteger,
                           ForeignKey('liprofile.id'),
                           nullable=False,
                           index=True)
    name          = Column(Unicode(STR_MAX))
    url           = Column(String(STR_MAX))

class WUSubject(SQLBase):
    __tablename__                = 'wusubject'
    id                           = Column(BigInteger, primary_key=True)
    title                        = Column(Unicode(STR_MAX))
    description                  = Column(Unicode(STR_MAX))
    average_salary               = Column(Unicode(STR_MAX))
    average_salary_rating        = Column(Unicode(STR_MAX))
    employed_furtherstudy        = Column(Unicode(STR_MAX))
    employed_furtherstudy_rating = Column(Unicode(STR_MAX))
    url                          = Column(Unicode(STR_MAX))
    courses_url                  = Column(Unicode(STR_MAX))
    alevels                      = relationship('WUALevel',
                                        cascade='all, delete-orphan')
    careers                      = relationship('WUCareer',
                                        cascade='all, delete-orphan')

class WUALevel(SQLBase):
    __tablename__ = 'wualevel'
    id            = Column(BigInteger, primary_key=True)
    subject_id    = Column(BigInteger,
                           ForeignKey('wusubject.id'),
                           nullable=False,
                           index=True)
    title         = Column(Unicode(STR_MAX))

class WUCareer(SQLBase):
    __tablename__ = 'wucareer'
    id            = Column(BigInteger, primary_key=True)
    subject_id    = Column(BigInteger,
                           ForeignKey('wusubject.id'),
                           nullable=False,
                           index=True)
    title         = Column(Unicode(STR_MAX))

class WUUniversity(SQLBase):
    __tablename__   = 'wuuniversity'
    id              = Column(BigInteger, primary_key=True)
    name            = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    city_id         = Column(BigInteger,
                            ForeignKey('wucity.id'),
                            nullable=True,
                            index=True)
    ucas_code       = Column(Unicode(STR_MAX))
    courses_url     = Column(Unicode(STR_MAX))
    description     = Column(Unicode(STR_MAX))
    website         = Column(Unicode(STR_MAX))
    further_study   = Column(BigInteger)
    further_study_r = Column(Unicode(STR_MAX))
    average_salary  = Column(BigInteger)
    average_salary_r= Column(Unicode(STR_MAX))
    student_score   = Column(BigInteger)
    student_score_r = Column(Unicode(STR_MAX))
    satisfaction    = Column(BigInteger)
    no_of_students  = Column(BigInteger)
    undergraduate   = Column(BigInteger)
    postgraduate    = Column(BigInteger)
    full_time       = Column(BigInteger)
    part_time       = Column(BigInteger)
    male            = Column(BigInteger)
    female          = Column(BigInteger)
    young           = Column(BigInteger)
    mature          = Column(BigInteger)
    uk              = Column(BigInteger)
    non_uk          = Column(BigInteger)
    lg_table_0      = Column(BigInteger)
    lg_table_0_ttl  = Column(BigInteger)
    lg_table_1      = Column(BigInteger)
    lg_table_1_ttl  = Column(BigInteger)
    lg_table_2      = Column(BigInteger)
    lg_table_2_ttl  = Column(BigInteger)
    url             = Column(Unicode(STR_MAX))
    characteristics \
         = relationship('WUCharacteristic',
                            cascade='all, delete-orphan')
    tags = relationship('WUTag',
                            cascade='all, delete-orphan')
    city = relationship('WUCity')

    __table_args__ = (UniqueConstraint('url'),)

class WUTag(SQLBase):
    __tablename__ = 'wutag'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    university_id = Column(BigInteger, 
                            ForeignKey('wuuniversity.id'),
                            nullable=False,
                            index=True)

class WUCharacteristic(SQLBase):
    __tablename__ = 'wucharacteristic'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    university_id = Column(BigInteger, 
                            ForeignKey('wuuniversity.id'),
                            nullable=False,
                            index=True)

class WUCity(SQLBase):
    __tablename__ = 'wucity'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX), 
                            index=True, 
                            nullable=False)

# database session class
class ParseDB(Session):
    def __init__(self, url=conf.PARSE_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)
