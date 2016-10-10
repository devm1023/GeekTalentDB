__all__ = [
    'WUSubject',
    'WUALevel',
    'WUCareer',
    'WUSubjectALevel',
    'WUSubjectCareer',
    'WhichUniDB'
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
    university_characteristics \
                    = relationship('WUUniversityCharacteristic',
                            cascade='all, delete-orphan')
    university_tags = relationship('WUUniversityTag',
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

class WUCharacteristic(SQLBase):
    __tablename__ = 'wucharacteristic'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    university_characteristic \
                  = relationship('WUUniversityCharacteristic',
                            cascade='all, delete-orphan')

class WUTag(SQLBase):
    __tablename__ = 'wutag'
    id            = Column(BigInteger, primary_key=True)
    name          = Column(Unicode(STR_MAX),
                            nullable=False,
                            index=True)
    university_tags = relationship('WUTag',
                           cascade='all, delete-orphan')
    

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
    
# database session class
class WhichUniDB(Session):
    def __init__(self, url=conf.WHICHUNI_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)