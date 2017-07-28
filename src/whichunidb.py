__all__ = [
    'WUSubject',
    'WUALevel',
    'WUCareer',
    'WUSubjectALevel',
    'WUSubjectCareer',
    'WUUniversity',
    'WUUniversityTag',
    'WUTag',
    'WUUniversityCharacteristic',
    'WUCharacteristic',
    'WUCity',
    'WULeagueTable',
    'WUUniversityLeagueTable',
    'WhichUniDB',
    'WUEntryRequirement',
    'WUCourseEntryRequirement',
    'WUStudyType',
    'WUCourse',
    'WUUniversitySubject',
    'WUStudiedBefore',
    'WUUniversitySubjectStudiedBefore',
    'WUSectorAfter',
    'WUUniversitySubjectSectorAfter',
    'WURating'
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

class WUSubject(SQLBase):
    __tablename__                = 'wusubject'
    id                           = Column(BigInteger, primary_key=True)
    title                        = Column(Unicode(STR_MAX))
    description                  = Column(Unicode(STR_MAX))
    average_salary               = Column(Integer)
    average_salary_rating        = Column(Unicode(STR_MAX))
    employed_furtherstudy        = Column(Float)
    employed_furtherstudy_rating = Column(Unicode(STR_MAX))
    url                          = Column(Unicode(STR_MAX))
    courses_url                  = Column(Unicode(STR_MAX))
    alevels                      = relationship('WUSubjectALevel',
                                        cascade='all, delete-orphan')
    careers                      = relationship('WUSubjectCareer',
                                        cascade='all, delete-orphan')

class WUSubjectALevel(SQLBase):
    __tablename__ = 'wusubjectalevel'
    id            = Column(BigInteger, primary_key=True)
    subject_id    = Column(BigInteger,
                            ForeignKey('wusubject.id'),
                            nullable=False,
                            index=True)
    alevel_id     = Column(BigInteger,
                            ForeignKey('wualevel.id'),
                            nullable=False,
                            index=True)
    alevel       = relationship('WUALevel')

class WUSubjectCareer(SQLBase):
    __tablename__ = 'wusubjectcareer'
    id            = Column(BigInteger, primary_key=True)
    subject_id    = Column(BigInteger,
                            ForeignKey('wusubject.id'),
                            nullable=False,
                            index=True)
    career_id     = Column(BigInteger,
                            ForeignKey('wucareer.id'),
                            nullable=False,
                            index=True)
    career        = relationship('WUCareer')

class WUALevel(SQLBase):
    __tablename__ = 'wualevel'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)

class WUCareer(SQLBase):
    __tablename__ = 'wucareer'
    id            = Column(BigInteger, primary_key=True)
    title         = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)


class WUCity(SQLBase):
    __tablename__ = 'wucity'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX), 
                            index=True, 
                            nullable=False)

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
    city            = relationship('WUCity')
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
    university_characteristics \
                    = relationship('WUUniversityCharacteristic',
                            cascade='all, delete-orphan')
    university_tags = relationship('WUUniversityTag',
                            cascade='all, delete-orphan')
    university_league_tables \
                    = relationship('WUUniversityLeagueTable',
                            cascade='all, delete-orphan')
    __table_args__ = (UniqueConstraint('url'),)

class WUUniversityCharacteristic(SQLBase):
    __tablename__     = 'wuuniversitycharacteristic'
    id                = Column(BigInteger, primary_key=True)
    university_id     = Column(BigInteger,
                                ForeignKey('wuuniversity.id'),
                                nullable=False,
                                index=True)
    characteristic_id = Column(BigInteger,
                                ForeignKey('wucharacteristic.id'),
                                nullable=False,
                                index=True)
    score             = Column(BigInteger)
    score_r           = Column(Unicode(STR_MAX))
    characteristic    = relationship('WUCharacteristic')

class WUCharacteristic(SQLBase):
    __tablename__ = 'wucharacteristic'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)

class WUTag(SQLBase):
    __tablename__ = 'wutag'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    

class WUUniversityTag(SQLBase):
    __tablename__ = 'wuuniversitytag'
    id            = Column(BigInteger, primary_key=True)
    university_id = Column(BigInteger, 
                            ForeignKey('wuuniversity.id'),
                            nullable=False,
                            index=True)
    tag_id        = Column(BigInteger,
                            ForeignKey('wutag.id'),
                            nullable=False,
                            index=True)
    tag         = relationship('WUTag')

class WULeagueTable(SQLBase):
    __tablename__ = 'wuleaguetable'
    id            = Column(BigInteger, 
                            primary_key=True)
    total         = Column(BigInteger, 
                            nullable=False)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)

class WUUniversityLeagueTable(SQLBase):
    __tablename__ = 'wuuniversityleaguetable'
    id            = Column(BigInteger, 
                            primary_key=True)
    university_id = Column(BigInteger, 
                            ForeignKey('wuuniversity.id'),
                            nullable=False,
                            index=True)
    league_table_id \
                  = Column(BigInteger, 
                            ForeignKey('wuleaguetable.id'),
                            nullable=False,
                            index=True)
    rating         = Column(BigInteger)
    league_table = relationship('WULeagueTable')

class WUCourse(SQLBase):
    __tablename__                   = 'wucourse'
    id                              = Column(BigInteger, 
                                                primary_key=True)
    university_id                   = Column(BigInteger,
                                                ForeignKey('wuuniversity.id'),
                                                nullable=True,
                                                index=True)
    title                           = Column(Unicode(STR_MAX),
                                                nullable=False,
                                                index=True)
    ucas_code                       = Column(Unicode(STR_MAX))
    url                             = Column(Unicode(STR_MAX))
    ucas_points_l                   = Column(BigInteger)
    ucas_points_h                   = Column(BigInteger)
    offers                          = Column(BigInteger)
    tuition_fee                     = Column(BigInteger)
    description                     = Column(Unicode(STR_MAX))
    modules                         = Column(Unicode(STR_MAX))
    entry_requirements              = relationship('WUCourseEntryRequirement',
                                                    cascade='all, delete-orphan')
    study_types                     = relationship('WUStudyType',
                                                    cascade='all, delete-orphan')
    university_subjects             = relationship('WUUniversitySubject',
                                                    cascade='all, delete-orphan')
    university                      = relationship('WUUniversity')

class WUStudyType(SQLBase):
    __tablename__       = 'wustudytype'
    id                  = Column(BigInteger, 
                            primary_key=True)
    course_id           = Column(BigInteger,
                            ForeignKey('wucourse.id'),
                            nullable=True,
                            index=True)
    qualification_name      = Column(Unicode(STR_MAX))
    duration                = Column(Unicode(STR_MAX))
    mode                    = Column(Unicode(STR_MAX))
    years                   = Column(Unicode(STR_MAX))
    

class WUCourseEntryRequirement(SQLBase):
    __tablename__       = 'wucourseentryrequirement'
    id                  = Column(BigInteger, 
                            primary_key=True)
    course_id           = Column(BigInteger,
                            ForeignKey('wucourse.id'),
                            nullable=True,
                            index=True)
    entryrequirement_id = Column(BigInteger,
                            ForeignKey('wuentryrequirement.id'),
                            nullable=True,
                            index=True)
    entry_requirement   = relationship('WUEntryRequirement')
    grades              = Column(Unicode(STR_MAX))
    text                = Column(Unicode(STR_MAX))

class WUEntryRequirement(SQLBase):
    __tablename__       = 'wuentryrequirement'
    id                  = Column(BigInteger, 
                            primary_key=True)
    name                = Column(Unicode(STR_MAX))

class WUUniversitySubject(SQLBase):
    __tablename__                = 'wuuniversitysubject'
    id                           = Column(BigInteger, 
                                    primary_key=True)
    university_id                = Column(BigInteger,
                                    ForeignKey('wuuniversity.id'),
                                    nullable=True,
                                    index=True)
    course_id                    = Column(BigInteger,
                                    ForeignKey('wucourse.id'),
                                    nullable=False,
                                    index=True)
    subject_id                   = Column(BigInteger,
                                    ForeignKey('wusubject.id'),
                                    nullable=True,
                                    index=True)
    subject                      = relationship('WUSubject')
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
    subject_name                 = Column(Unicode(STR_MAX))
    studied_before               = relationship('WUUniversitySubjectStudiedBefore',
                                                    cascade='all, delete-orphan')
    sectors_after                = relationship('WUUniversitySubjectSectorAfter',
                                                    cascade='all, delete-orphan')
    ratings                      = relationship('WURating',
                                                    cascade='all, delete-orphan')

class WUUniversitySubjectSectorAfter(SQLBase):
    __tablename__ = 'wuuniversitysubjectsectorafter'
    id             = Column(BigInteger, primary_key=True)
    percent       = Column(BigInteger)
    university_subject_id = \
                     Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=False,
                            index=True)
    sector_after_id = \
                     Column(BigInteger,
                            ForeignKey('wusectorafter.id'),
                            nullable=False,
                            index=True)
    sector_after = relationship('WUSectorAfter')

class WUSectorAfter(SQLBase):
    __tablename__ = 'wusectorafter'
    id             = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX))

class WURating(SQLBase):
    __tablename__       = 'wurating'
    id                  = Column(BigInteger, primary_key=True)
    name                = Column(Unicode(STR_MAX))
    rating              = Column(BigInteger)
    university_subject_id \
                        = Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=False,
                            index=True)

class WUUniversitySubjectStudiedBefore(SQLBase):
    __tablename__       = 'wuuniversitysubjectstudiedbefore'
    id                  = Column(BigInteger, 
                            primary_key=True)
    university_subject_id \
                        = Column(BigInteger,
                            ForeignKey('wuuniversitysubject.id'),
                            nullable=True,
                            index=True)
    studied_before_id   = Column(BigInteger,
                            ForeignKey('wustudiedbefore.id'),
                            nullable=True,
                            index=True)
    percent             = Column(BigInteger)
    common_grade        = Column(Unicode(STR_MAX))
    common_grade_percent= Column(BigInteger)
    studied_before      = relationship('WUStudiedBefore')


class WUStudiedBefore(SQLBase):
    __tablename__   = 'wustudiedbefore'
    id              = Column(BigInteger, 
                            primary_key=True)
    name            = Column(Unicode(STR_MAX))

# database session class
class WhichUniDB(Session):
    def __init__(self, url=conf.PARSE_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)