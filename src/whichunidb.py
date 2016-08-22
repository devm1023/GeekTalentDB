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

# database session class
class WhichUniDB(Session):
    def __init__(self, url=conf.WHICHUNI_DB,
                 engine_args=[], engine_kwargs={}, **kwargs):
        Session.__init__(self, url=url, metadata=SQLBase.metadata,
                         engine_args=engine_args, engine_kwargs=engine_kwargs,
                         **kwargs)