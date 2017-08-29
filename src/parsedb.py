__all__ = [
    'LIProfile',
    'LIExperience',
    'LIEducation',
    'LIGroup',
    'LISkill',
    'WUSubject',
    'WUALevel',
    'WUCareer',
    'WULeagueTable',
    'WUCity',
    'WUCourse',
    'ADZJob',
    'ParseDB',
    'WUUniversitySubject'
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
    url             = Column(Unicode(STR_MAX))
    league_tables  \
         = relationship('WULeagueTable',
                            cascade='all, delete-orphan')
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

class WULeagueTable(SQLBase):
    __tablename__ = 'wuleaguetable'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    total         = Column(BigInteger, nullable=False)
    rating        = Column(BigInteger, nullable=False)
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
    score         = Column(BigInteger)
    rating        = Column(Unicode(STR_MAX))
    university_id = Column(BigInteger, 
                            ForeignKey('wuuniversity.id'),
                            nullable=False,
                            index=True)

class WUCity(SQLBase):
    __tablename__  = 'wucity'
    id             = Column(BigInteger, primary_key=True)
    name           = Column(Unicode(STR_MAX), 
                            index=True, 
                            nullable=False)

class WUCourse(SQLBase):
    __tablename__                       = 'wucourse'
    id                               = Column(BigInteger, primary_key=True)
    title                            = Column(Unicode(STR_MAX),
                                                nullable=False,
                                                index=True)
    ucas_code                       = Column(Unicode(STR_MAX))
    url                             = Column(Unicode(STR_MAX))
    university_name                 = Column(Unicode(STR_MAX))
    ucas_points_l                   = Column(BigInteger)
    ucas_points_h                   = Column(BigInteger)
    offers                          = Column(BigInteger)
    tuition_fee                     = Column(BigInteger)
    description                     = Column(Unicode(STR_MAX))
    modules                         = Column(Unicode(STR_MAX))
    entry_requirements              = relationship('WUEntryRequirement',
                                                    cascade='all, delete-orphan')
    study_types                     = relationship('WUStudyType',
                                                    cascade='all, delete-orphan')
    subjects                        = relationship('WUUniversitySubject',
                                                    cascade='all, delete-orphan')
    
class WUStudyType(SQLBase):
    __tablename__           = 'wustudytype'
    id                      = Column(BigInteger, primary_key=True)
    qualification_name      = Column(Unicode(STR_MAX))
    duration                = Column(Unicode(STR_MAX))
    mode                    = Column(Unicode(STR_MAX))
    years                   = Column(Unicode(STR_MAX))
    course_id               = Column(BigInteger,
                                    ForeignKey('wucourse.id'),
                                    nullable=False,
                                    index=True)

class WUEntryRequirement(SQLBase):
    __tablename__   = 'wuentryrequirement'
    id              = Column(BigInteger, primary_key=True)
    name            = Column(Unicode(STR_MAX))
    grades          = Column(Unicode(STR_MAX))
    text            = Column(Unicode(STR_MAX))
    course_id       = Column(BigInteger,
                            ForeignKey('wucourse.id'),
                            nullable=False,
                            index=True)

class WUUniversitySubject(SQLBase):
    __tablename__                = 'wuuniversitysubject'
    id                           = Column(BigInteger, primary_key=True)
    course_id                    = Column(BigInteger,
                                        ForeignKey('wucourse.id'),
                                        nullable=False,
                                        index=True)
    student_score                = Column(BigInteger)
    student_score_rating         = Column(Unicode(STR_MAX))
    employed_furtherstudy        = Column(BigInteger)
    employed_furtherstudy_rating = Column(Unicode(STR_MAX))
    average_salary_rating        = Column(Unicode(STR_MAX))
    average_salary               = Column(BigInteger)
    uk                           = Column(BigInteger)
    non_uk                       = Column(BigInteger)
    male                         = Column(BigInteger)
    female                       = Column(BigInteger)
    full_time                    = Column(BigInteger)
    part_time                    = Column(BigInteger)
    typical_ucas_points          = Column(BigInteger)
    twotoone_or_above            = Column(BigInteger)
    satisfaction                 = Column(BigInteger)
    dropout_rate                 = Column(BigInteger)
    university_name              = Column(Unicode(STR_MAX))
    subject_name                 = Column(Unicode(STR_MAX))
    studied_before               = relationship('WUStudiedBefore',
                                                    cascade='all, delete-orphan')
    sectors_after                = relationship('WUSectorAfter',
                                                    cascade='all, delete-orphan')
    ratings                      = relationship('WURating',
                                                    cascade='all, delete-orphan')

class WURating(SQLBase):
    __tablename__   = 'wurating'
    id              = Column(BigInteger, primary_key=True)
    name            = Column(Unicode(STR_MAX))
    rating          = Column(BigInteger)
    university_subject_id = \
                      Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=False,
                            index=True)

class WUStudiedBefore(SQLBase):
    __tablename__  = 'wustudiedbefore'
    id             = Column(BigInteger, primary_key=True)
    name           = Column(Unicode(STR_MAX))
    percent        = Column(BigInteger)
    common_grade   = Column(Unicode(STR_MAX))
    common_grade_percent = Column(BigInteger)
    university_subject_id = \
                     Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=False,
                            index=True)
    
class WUSectorAfter(SQLBase):
    __tablename__ = 'wusectorafter'
    id             = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX))
    percent       = Column(BigInteger)
    university_subject_id = \
                     Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=False,
                            index=True)

# TODO: merge parse and datoin job tables?
class ADZJob(SQLBase):
    __tablename__ = 'adzjob'
    id            = Column(BigInteger, primary_key=True)
    timestamp     = Column(DateTime, index=True, nullable=False)
    adref         = Column(String(STR_MAX), index=True, nullable=False)
    description   = Column(Unicode(STR_MAX))
    url           = Column(Unicode(STR_MAX))

# database session class
class ParseDB(Session):
    def __init__(self, url=conf.PARSE_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)
